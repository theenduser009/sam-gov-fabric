# Fabric notebook source
# Prepared by Codex (OpenAI)

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# PARAMETERS CELL ********************

# Inclusive signed dates for the one-year backfill.
signed_from = "2025-11-20"
signed_to = "2026-11-19"
max_api_requests = 10

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Paste the active SAM.gov Public API Key in this private Fabric notebook.
sam_api_key = ""

import hashlib
import json
from collections import deque
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import requests

SEARCH_URL = "https://api.sam.gov/contract-awards/v1/search"
LAKEHOUSE_FILES = Path("/lakehouse/default/Files")
PAGE_SIZE = 100
MAX_SEARCH_RECORDS = 400_000


class LoadPaused(Exception):
    """Saved pages can be resumed on a later run."""


class SamSearch:
    def __init__(self, session, api_key, request_limit):
        self.session = session
        self.api_key = api_key
        self.remaining = request_limit

    def page(self, kind, start, end, offset):
        if not self.remaining:
            raise LoadPaused("The notebook used its request budget. Rerun to resume.")

        self.remaining -= 1
        params = {
            "api_key": self.api_key,
            "awardOrIDV": kind,
            "dateSigned": f"[{start:%m/%d/%Y},{end:%m/%d/%Y}]",
            "limit": PAGE_SIZE,
        }
        if offset:
            params["offset"] = offset

        try:
            with self.session.get(
                SEARCH_URL, params=params, timeout=(15, 90), allow_redirects=False
            ) as response:
                status = response.status_code
                if status == 204:
                    return [], 0
                if status == 429:
                    raise LoadPaused("SAM.gov rate-limited the request. Resume after the allowance resets.")
                if status != 200:
                    raise RuntimeError(
                        f"{kind} {start} through {end}: JSON search returned HTTP {status}."
                    )
                try:
                    body = response.json()
                    total = int(body["totalRecords"])
                    records = body.get("awardSummary", [] if total == 0 else None)
                except (TypeError, KeyError, ValueError, AttributeError):
                    raise RuntimeError("SAM.gov returned an unexpected JSON response.") from None
        except requests.RequestException:
            raise RuntimeError(f"No usable response from SAM.gov for {kind}.") from None

        if total < 0 or not isinstance(records, list) or not all(
            isinstance(record, dict) for record in records
        ):
            raise RuntimeError("SAM.gov returned an unexpected awardSummary or totalRecords value.")
        return records, total

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_json_file(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{uuid4().hex}.part")
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def signed_months(start, end):
    while end >= start:
        low = max(start, end.replace(day=1))
        yield low, end
        if low == start:
            break
        end = low - timedelta(days=1)


def split_window(kind, start, end):
    if start == end:
        raise RuntimeError(f"{kind} {start} exceeded SAM.gov's search limit for one day.")
    middle = start + (end - start) // 2
    return [(kind, middle + timedelta(days=1), end), (kind, start, middle)]


def land_page(client, root, run, kind, start, end):
    window = f"signed_from={start}/signed_to={end}"
    marker = root / "_checkpoints/json_search" / window / f"{kind.lower()}.json"
    previous = json.loads(marker.read_text(encoding="utf-8")) if marker.is_file() else {}
    if previous.get("complete"):
        return []
    if previous.get("split"):
        return split_window(kind, start, end)

    page = previous.get("next_page", 0)
    offset = previous.get("next_offset", page)
    records, total = client.page(kind, start, end, offset)
    if page and total != previous["total"]:
        raise RuntimeError(
            f"{kind} {start} through {end}: the match count changed during paging. "
            "The saved pages are available. Use a different signed-date range for a new load."
        )
    if total > MAX_SEARCH_RECORDS:
        children = split_window(kind, start, end)
        write_json_file(marker, {"split": True})
        return children

    fingerprint = hashlib.sha256(
        json.dumps(records, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    if records and fingerprint == previous.get("last_page_sha256"):
        if page == 1 and offset == 1:
            # Some responses treat offset 1 as the first page when offset is omitted.
            write_json_file(marker, {**previous, "next_offset": 2})
            return [(kind, start, end)]
        raise RuntimeError(f"{kind}: SAM.gov repeated the previous page. Saved progress was kept.")

    expected = min(PAGE_SIZE, max(0, total - page * PAGE_SIZE))
    if len(records) != expected:
        raise RuntimeError(
            f"{kind} page {page} contained {len(records)} records. "
            f"The reported total implies {expected}. Saved progress was kept."
        )

    if records:
        target = run / f"record_type={kind.lower()}" / window / f"page-{page:04d}.json"
        write_json_file(target, {
            "awardSummary": records,
            "totalRecords": total,
            "offset": offset,
            "limit": PAGE_SIZE,
        })
        print(f"Saved {len(records):,} {kind} records to {target.relative_to(LAKEHOUSE_FILES).as_posix()}")

    complete = page * PAGE_SIZE + len(records) == total
    write_json_file(marker, {
        "next_page": page + 1,
        "next_offset": offset + 1,
        "total": total,
        "complete": complete,
        "last_page_sha256": fingerprint,
    })
    if complete:
        if total == 0:
            print(f"No {kind} records for {start} through {end}.")
        return []
    return [(kind, start, end)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ingest():
    key = sam_api_key.strip()
    if not key:
        raise ValueError("Enter the SAM.gov key in sam_api_key in the second code cell.")
    if not LAKEHOUSE_FILES.is_dir():
        raise RuntimeError("Attach the destination Lakehouse as this notebook's default Lakehouse.")

    start = date.fromisoformat(signed_from.strip())
    end = date.fromisoformat(signed_to.strip())
    request_limit = int(max_api_requests)
    if start > end or request_limit < 1:
        raise ValueError("Check the signed-date range and request budget in the first code cell.")

    now = datetime.now(timezone.utc)
    root = LAKEHOUSE_FILES / "raw/sam/contract-awards"
    run = root / f"ingested_date={now:%Y-%m-%d}" / f"run={now:%Y%m%dT%H%M%SZ}-{uuid4().hex[:12]}" / "source=json_search"

    status = "complete"
    with requests.Session() as session:
        client = SamSearch(session, key, request_limit)
        try:
            for low, high in signed_months(start, end):
                pending = deque((kind, low, high) for kind in ("Award", "IDV"))
                while pending:
                    kind, window_start, window_end = pending.popleft()
                    pending.extend(land_page(client, root, run, kind, window_start, window_end))
        except LoadPaused as paused:
            status = "paused"
            print(paused)

    receipt = {
        "status": status,
        "signed_from": str(start),
        "signed_to": str(end),
        "ingested_utc": now.isoformat(),
        "api_requests": request_limit - client.remaining,
    }
    write_json_file(run / "_RUN.json", receipt)
    if status == "complete":
        write_json_file(run / "_SUCCESS.json", receipt)

    print(f"Run {status}. Lakehouse folder: Files/{run.relative_to(LAKEHOUSE_FILES).as_posix()}")


ingest()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Preview the JSON already saved in the default Lakehouse.
from collections import Counter
from pathlib import Path
import json

# Leave blank to preview the newest ingestion date with saved pages.
preview_ingested_date = ""


def preview_saved_sam_pages():
    root = Path("/lakehouse/default/Files/raw/sam/contract-awards")
    if preview_ingested_date:
        folders = [root / f"ingested_date={preview_ingested_date}"]
    else:
        folders = sorted(root.glob("ingested_date=*"), reverse=True)

    for folder in folders:
        pages = sorted(folder.glob("run=*/source=json_search/record_type=*/signed_from=*/signed_to=*/page-*.json"))
        if pages:
            break
    else:
        print("No saved SAM.gov pages found. Check the default Lakehouse and ingestion date.")
        return

    counts = Counter()
    sample = []
    for page in pages:
        record_type = page.parents[2].name.split("=", 1)[1].upper()
        records = json.loads(page.read_text(encoding="utf-8"))["awardSummary"]
        for record in records[:max(0, 5 - counts[record_type])]:
            details = record.get("awardDetails") or {}
            awardee = (details.get("awardeeData") or {}).get("awardeeHeader") or {}
            sample.append((
                record_type,
                (record.get("contractId") or {}).get("piid"),
                (details.get("dates") or {}).get("dateSigned"),
                awardee.get("legalBusinessName"),
                (details.get("dollars") or {}).get("actionObligation"),
            ))
        counts[record_type] += len(records)

    print(f"{folder.name} | {len(pages)} JSON files | {counts['AWARD']} Award rows | {counts['IDV']} IDV rows")
    if sample:
        display(spark.createDataFrame(
            sample,
            "record_type STRING, contract_number STRING, signed_date STRING, vendor STRING, obligated_usd STRING",
        ))


if notebookutils.runtime.context.get("isForInteractive", False):
    preview_saved_sam_pages()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
