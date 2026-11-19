# Fabric notebook source
# Prepared by Codex (OpenAI)

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# PARAMETERS CELL ********************

# Inclusive signed dates. An empty end date is set on the first run.
signed_from = "1978-10-01"
signed_to = ""
max_api_requests = 10
load_mode = "bulk_json"

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
from shutil import copyfileobj
from time import sleep
from uuid import uuid4
from zipfile import ZipFile

import requests

SEARCH_URL = "https://api.sam.gov/contract-awards/v1/search"
DOWNLOAD_URL = "https://api.sam.gov/contract-awards/v1/download"
LAKEHOUSE_FILES = Path("/lakehouse/default/Files")
PAGE_SIZE = 100
MAX_SEARCH_RECORDS = 400_000
BULK_POLL_SECONDS = 60


class LoadPaused(Exception):
    """Saved pages can be resumed on a later run."""


class SamClient:
    def __init__(self, session, api_key, request_limit):
        self.session = session
        self.api_key = api_key
        self.remaining = request_limit

    def require_budget(self):
        if not self.remaining:
            raise LoadPaused("The notebook used its request budget. Rerun to resume.")

    def get(self, url, params, *, stream=False):
        self.require_budget()
        self.remaining -= 1
        try:
            response = self.session.get(
                url,
                params={**params, "api_key": self.api_key},
                timeout=(15, 120),
                allow_redirects=False,
                stream=stream,
            )
        except requests.RequestException:
            raise RuntimeError("No usable response from SAM.gov.") from None
        if response.status_code == 429:
            response.close()
            raise LoadPaused("SAM.gov rate-limited the request. Resume after the allowance resets.")
        return response

    def page(self, kind, start, end, offset):
        params = {
            "awardOrIDV": kind,
            "dateSigned": f"[{start:%m/%d/%Y},{end:%m/%d/%Y}]",
            "limit": PAGE_SIZE,
        }
        if offset:
            params["offset"] = offset

        with self.get(SEARCH_URL, params) as response:
            status = response.status_code
            if status == 204:
                return [], 0
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


def signed_years(start, end):
    while end >= start:
        low = max(start, date(end.year, 1, 1))
        yield low, end
        if low == start:
            break
        end = low - timedelta(days=1)


def split_window(kind, start, end):
    if start == end:
        raise RuntimeError(f"{kind} {start} exceeded SAM.gov's record limit for one day.")
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

def bulk_error_text(response):
    try:
        return response.text.lower()
    except requests.RequestException:
        return ""


def request_bulk(client, kind, start, end):
    params = {
        "awardOrIDV": kind,
        "dateSigned": f"[{start:%m/%d/%Y},{end:%m/%d/%Y}]",
        "format": "json",
    }
    with client.get(SEARCH_URL, params) as response:
        status = response.status_code
        if status == 204:
            return {"complete": True}
        if status == 400:
            message = bulk_error_text(response)
            if "count exceeded error" in message or "exceeded the maximum allowable limit" in message:
                return {"split": True}
        if status not in (200, 202):
            raise RuntimeError(f"{kind} {start} through {end}: bulk JSON request returned HTTP {status}.")
        try:
            token = response.json().get("exportToken")
        except (ValueError, AttributeError):
            token = None
        if not isinstance(token, str) or not token or client.api_key in token:
            raise RuntimeError(f"SAM.gov did not return a usable {kind} bulk export token.")
    poll_after = datetime.now(timezone.utc) + timedelta(seconds=BULK_POLL_SECONDS)
    return {"token": token, "poll_after": poll_after.isoformat()}


def download_bulk(client, kind, token, folder):
    with client.get(DOWNLOAD_URL, {"token": token}, stream=True) as response:
        status = response.status_code
        if status in (202, 204):
            return {"pending": True}
        if status == 400:
            message = bulk_error_text(response)
            if "not generated yet" in message:
                return {"pending": True}
            if "token is expired" in message:
                return {"expired": True}
        if status != 200:
            raise RuntimeError(f"{kind} bulk download returned HTTP {status}.")

        staged = folder / f".{uuid4().hex}.part"
        try:
            response.raw.decode_content = True
            prefix = response.raw.read(8192)
            preview = prefix.lstrip(b"\xef\xbb\xbf \t\r\n")
            waiting = b"file generation is in progress" in preview.lower()
            if waiting and b'"awardSummary"' not in preview:
                return {"pending": True}
            if preview.startswith(b"{") and b'"awardSummary"' in preview:
                suffix = ".json"
            elif prefix.startswith(b"PK\x03\x04"):
                suffix = ".zip"
            elif prefix.startswith(b"\x1f\x8b"):
                suffix = ".json.gz"
            else:
                raise ValueError("Unexpected download format")

            folder.mkdir(parents=True, exist_ok=True)
            with staged.open("wb") as file:
                file.write(prefix)
                copyfileobj(response.raw, file, length=1024 * 1024)
            if suffix == ".json":
                with staged.open("rb") as file:
                    file.seek(-min(8192, staged.stat().st_size), 2)
                    if not file.read().rstrip().endswith(b"}"):
                        raise ValueError("The JSON response ended before the closing brace")
            elif suffix == ".zip":
                with ZipFile(staged) as archive:
                    if not any(not item.is_dir() for item in archive.infolist()):
                        raise ValueError("The archive does not contain an extract")
            target = folder / f"extract{suffix}"
            staged.replace(target)
            return {"file": target}
        except Exception:
            raise RuntimeError(f"Could not save a complete {kind} bulk extract.") from None
        finally:
            staged.unlink(missing_ok=True)


def land_bulk(client, root, run, kind, start, end):
    window = f"signed_from={start}/signed_to={end}"
    marker = root / "_checkpoints/bulk_json" / window / f"{kind.lower()}.json"
    previous = json.loads(marker.read_text(encoding="utf-8")) if marker.is_file() else {}
    if previous.get("complete"):
        if "path" not in previous:
            return []
        file = root / previous["path"]
        if file.is_file() and file.stat().st_size == previous["bytes"]:
            return []
    if previous.get("split"):
        return split_window(kind, start, end)

    token = previous.get("token")
    if not token:
        progress = request_bulk(client, kind, start, end)
        write_json_file(marker, progress)
        if progress.get("complete"):
            print(f"No {kind} records for {start} through {end}.")
            return []
        if progress.get("split"):
            return split_window(kind, start, end)
        print(f"Requested {kind} bulk extract for {start} through {end}.")
        return [(kind, start, end)]

    client.require_budget()
    remaining_wait = (datetime.fromisoformat(previous["poll_after"]) - datetime.now(timezone.utc)).total_seconds()
    if remaining_wait > 0:
        sleep(min(remaining_wait, BULK_POLL_SECONDS))
    result = download_bulk(client, kind, token, run / f"record_type={kind.lower()}" / window)
    if result.get("pending"):
        next_poll = datetime.now(timezone.utc) + timedelta(seconds=BULK_POLL_SECONDS)
        write_json_file(marker, {**previous, "poll_after": next_poll.isoformat()})
        return [(kind, start, end)]
    if result.get("expired"):
        write_json_file(marker, {})
        return [(kind, start, end)]

    file = result["file"]
    write_json_file(marker, {
        "complete": True,
        "path": file.relative_to(root).as_posix(),
        "bytes": file.stat().st_size,
    })
    print(f"Saved {kind} extract to {file.relative_to(LAKEHOUSE_FILES).as_posix()}")
    return []

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
    if load_mode not in ("bulk_json", "json_search"):
        raise ValueError("Set load_mode to bulk_json or json_search.")
    request_limit = int(max_api_requests)
    now = datetime.now(timezone.utc)
    root = LAKEHOUSE_FILES / "raw/sam/contract-awards"
    snapshot = root / "_checkpoints" / load_mode / f"snapshot-from-{start}.json"
    if signed_to.strip():
        end = date.fromisoformat(signed_to.strip())
    elif snapshot.is_file():
        end = date.fromisoformat(json.loads(snapshot.read_text(encoding="utf-8"))["signed_to"])
    else:
        end = now.date()
    if start > end or request_limit < 1:
        raise ValueError("Check the signed-date range and request budget in the first code cell.")
    if not signed_to.strip() and not snapshot.is_file():
        write_json_file(snapshot, {"signed_to": str(end)})

    windows, load = (signed_years, land_bulk) if load_mode == "bulk_json" else (signed_months, land_page)
    run = root / f"ingested_date={now:%Y-%m-%d}" / f"run={now:%Y%m%dT%H%M%SZ}-{uuid4().hex[:12]}" / f"source={load_mode}"

    status = "complete"
    failure = None
    with requests.Session() as session:
        client = SamClient(session, key, request_limit)
        try:
            for low, high in windows(start, end):
                pending = deque((kind, low, high) for kind in ("Award", "IDV"))
                while pending:
                    kind, window_start, window_end = pending.popleft()
                    pending.extend(load(client, root, run, kind, window_start, window_end))
        except LoadPaused as paused:
            status = "paused"
            print(paused)
        except RuntimeError as error:
            status = "failed"
            failure = error

    receipt = {
        "status": status,
        "load_mode": load_mode,
        "signed_from": str(start),
        "signed_to": str(end),
        "ingested_utc": now.isoformat(),
        "api_requests": request_limit - client.remaining,
    }
    write_json_file(run / "_RUN.json", receipt)
    if status == "complete":
        write_json_file(run / "_SUCCESS.json", receipt)

    print(f"Run {status}. Lakehouse folder: Files/{run.relative_to(LAKEHOUSE_FILES).as_posix()}")
    if failure:
        raise failure


ingest()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
