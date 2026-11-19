"""Paste into one Fabric cell after running the cell that sets sam_api_key."""

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import requests

# Inclusive signed dates for the first load.
json_signed_from = "2026-11-01"
json_signed_to = "2026-11-19"

# Two requests normally land one Award page and one IDV page.
json_request_budget = 2

SEARCH_URL = "https://api.sam.gov/contract-awards/v1/search"
PAGE_SIZE = 100
SEARCH_RECORD_CAP = 400_000


def write_json_file(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.part")
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def search_award_page(session, api_key, kind, start, end, page):
    try:
        with session.get(
            SEARCH_URL,
            params={
                "api_key": api_key,
                "awardOrIDV": kind,
                "dateSigned": f"[{start:%m/%d/%Y},{end:%m/%d/%Y}]",
                "limit": PAGE_SIZE,
                "offset": page,
            },
            timeout=(15, 90),
            allow_redirects=False,
        ) as response:
            if response.status_code == 204:
                return [], 0
            if response.status_code == 429:
                raise RuntimeError("SAM.gov rate limit reached. Resume after the allowance resets.")
            if response.status_code != 200:
                raise RuntimeError(f"{kind} JSON search returned HTTP {response.status_code}.")
            try:
                payload = response.json()
                return payload["awardSummary"], int(payload["totalRecords"])
            except (ValueError, TypeError, KeyError):
                raise RuntimeError(f"{kind} search did not return the expected SAM.gov JSON.") from None
    except requests.RequestException:
        raise RuntimeError(f"{kind} JSON search did not return a usable response.") from None


def load_sam_json():
    api_key = sam_api_key.strip()
    if not api_key:
        raise ValueError("Run the cell that sets sam_api_key first.")
    start = date.fromisoformat(json_signed_from)
    end = date.fromisoformat(json_signed_to)
    budget = int(json_request_budget)
    if start > end or budget < 1:
        raise ValueError("Check the signed-date range and request budget.")

    lakehouse_files = Path("/lakehouse/default/Files")
    if not lakehouse_files.is_dir():
        raise RuntimeError("Attach the intended Lakehouse as the default Lakehouse.")

    root = lakehouse_files / "raw/sam/contract-awards"
    now = datetime.now(timezone.utc)
    run = root / f"ingested_date={now:%Y-%m-%d}" / f"run={now:%Y%m%dT%H%M%SZ}-{uuid4().hex}" / "source=json_search"
    window = f"signed_from={start}/signed_to={end}"
    pending = {}
    for kind in ("Award", "IDV"):
        marker = root / "_checkpoints/json_search" / window / f"{kind.lower()}.json"
        progress = json.loads(marker.read_text(encoding="utf-8")) if marker.is_file() else {}
        if not progress.get("complete"):
            pending[kind] = (marker, progress)

    with requests.Session() as session:
        while pending and budget:
            for kind in list(pending):
                if not budget:
                    break
                marker, previous = pending[kind]
                page = previous.get("next_page", 0)
                budget -= 1
                records, total = search_award_page(session, api_key, kind, start, end, page)
                if total > SEARCH_RECORD_CAP:
                    raise RuntimeError(f"{kind} has {total:,} matches. Split the signed dates before paging.")
                if "total" in previous and previous["total"] != total:
                    raise RuntimeError(
                        f"{kind} match count changed from {previous['total']:,} to {total:,}. "
                        "Use a new signed-date window before continuing."
                    )
                if not isinstance(records, list) or len(records) != min(PAGE_SIZE, max(total - page * PAGE_SIZE, 0)):
                    raise RuntimeError(f"{kind} page {page} returned an unexpected record count.")
                serialized = json.dumps(records, sort_keys=True, ensure_ascii=False)
                fingerprint = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
                if records and fingerprint == previous.get("last_page_sha256"):
                    raise RuntimeError(f"{kind} page {page} repeated the previous page. No progress was recorded.")

                if records:
                    target = run / f"record_type={kind.lower()}" / window / f"page-{page:04d}.json"
                    write_json_file(target, {
                        "awardSummary": records,
                        "totalRecords": total,
                        "offset": page,
                        "limit": PAGE_SIZE,
                    })
                    print(f"Saved {len(records)} {kind} records to {target.relative_to(lakehouse_files).as_posix()}")
                complete = page * PAGE_SIZE + len(records) == total
                progress = {
                    "next_page": page + 1,
                    "total": total,
                    "complete": complete,
                    "last_page_sha256": fingerprint,
                }
                write_json_file(marker, progress)
                if complete:
                    del pending[kind]
                    print(f"{kind} date window complete with {total:,} records.")
                else:
                    pending[kind] = (marker, progress)

    if pending:
        print("Run paused at its request budget. Rerun this cell to continue.")
    else:
        print("Both record types are complete for the selected dates.")


load_sam_json()
