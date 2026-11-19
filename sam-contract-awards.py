# Fabric notebook source
# Prepared by Codex (OpenAI)

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# PARAMETERS CELL ********************

# An empty end date uses the UTC date of the first run for this start date.
signed_from = "1978-10-01"
signed_to = ""
max_api_requests = 10

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Paste your SAM.gov API key between the quotes in your private Fabric notebook.
sam_api_key = ""

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from shutil import copyfileobj
from time import sleep
from uuid import uuid4

import requests

API_ROOT = "https://api.sam.gov/contract-awards/v1"
LAKEHOUSE_FILES = Path("/lakehouse/default/Files")
POLL_DELAYS = (30, 90, 180, 300)


class BackfillPaused(Exception):
    """The run can continue later from the saved partitions."""


class ExtractTooLarge(Exception):
    """SAM.gov requires a smaller date range."""


class SamClient:
    def __init__(self, session, api_key, request_limit):
        self.session = session
        self.api_key = api_key
        self.remaining = request_limit

    def get(self, endpoint, params, *, stream=False):
        self.remaining -= 1
        try:
            response = self.session.get(
                f"{API_ROOT}/{endpoint}",
                params={**params, "api_key": self.api_key},
                timeout=(15, 120),
                stream=stream,
                allow_redirects=False,
            )
        except requests.RequestException:
            raise RuntimeError(f"SAM.gov {endpoint} request failed.") from None
        if response.status_code == 429:
            response.close()
            raise BackfillPaused("SAM.gov rate limit reached. Resume after the limit resets.")
        return response


def response_text(response):
    try:
        return response.text.lower()
    except requests.RequestException:
        raise RuntimeError("Could not read the SAM.gov export status.") from None


def request_export(client, kind, start, end):
    params = {
        "awardOrIDV": kind,
        "dateSigned": f"[{start:%m/%d/%Y},{end:%m/%d/%Y}]",
        "format": "csv",
    }
    with client.get("search", params) as response:
        if response.status_code == 204:
            return None
        if response.status_code == 400:
            error = response_text(response)
            if "count exceeded error" in error or "exceeded the maximum allowable limit" in error:
                raise ExtractTooLarge()
        if response.status_code not in (200, 202):
            raise RuntimeError(f"{kind} {start} to {end}: export request returned HTTP {response.status_code}.")
        try:
            token = response.json().get("exportToken")
        except (ValueError, AttributeError):
            token = None
        if not isinstance(token, str) or not token:
            raise RuntimeError(f"SAM.gov did not return an export token for {kind} {start} to {end}.")
        return token


def download_export(client, kind, token, folder):
    for delay in POLL_DELAYS:
        sleep(delay)
        with client.get("download", {"token": token}, stream=True) as response:
            if response.status_code == 202:
                continue
            if response.status_code == 400 and "file is not generated yet" in response_text(response):
                continue
            if response.status_code != 200:
                raise RuntimeError(f"{kind} download returned HTTP {response.status_code}.")

            content_type = response.headers.get("Content-Type", "").partition(";")[0].strip().lower()
            if content_type == "application/json":
                message = response_text(response)
                if "file is not generated yet" in message or "generation is in progress" in message:
                    continue
                raise RuntimeError(f"SAM.gov returned a status response instead of the {kind} extract.")

            response.raw.decode_content = True
            staged = folder / f"{kind.lower()}.part"
            try:
                prefix = response.raw.read(512)
                first_line = prefix.lstrip(b"\xef\xbb\xbf \t\r\n").partition(b"\n")[0]
                if prefix.startswith((b"PK\x03\x04", b"PK\x05\x06")):
                    extension = "zip"
                elif content_type in (
                    "", "text/csv", "application/csv", "text/plain",
                    "application/octet-stream", "application/vnd.ms-excel",
                ) and b"," in first_line and not first_line.startswith((b"{", b"<", b"[")):
                    extension = "csv"
                else:
                    raise ValueError("Unexpected extract format.")

                folder.mkdir(parents=True, exist_ok=True)
                with staged.open("wb") as output:
                    output.write(prefix)
                    copyfileobj(response.raw, output, length=1024 * 1024)
                target = folder / f"{kind.lower()}.{extension}"
                staged.replace(target)
                return target
            except Exception:
                staged.unlink(missing_ok=True)
                raise RuntimeError(f"Could not save a complete CSV or ZIP for {kind}.") from None
    raise BackfillPaused(f"The {kind} export was still pending. Completed windows are saved.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(f".{path.name}.{uuid4().hex}.part")
    try:
        staged.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        staged.replace(path)
    finally:
        staged.unlink(missing_ok=True)


def snapshot_end(state, start, now):
    if signed_to.strip():
        return date.fromisoformat(signed_to.strip())

    path = state / f"snapshot-from-{start}.json"
    if path.exists():
        return date.fromisoformat(json.loads(path.read_text(encoding="utf-8"))["signed_to"])
    end = now.date()
    if end >= start:
        write_json(path, {"signed_to": str(end)})
    return end


def month_windows(start, end):
    while end >= start:
        low = max(start, end.replace(day=1))
        yield low, end
        end = low - timedelta(days=1)


def load_window(client, root, state, run_dir, kind, start, end):
    partition = f"record_type={kind.lower()}/signed_from={start}/signed_to={end}"
    checkpoint = state / partition / "status.json"
    previous = json.loads(checkpoint.read_text(encoding="utf-8")) if checkpoint.exists() else {}
    status = previous.get("status")
    if status == "empty":
        return
    if status == "saved":
        file = root / previous["path"]
        if file.is_file() and file.stat().st_size == previous["bytes"]:
            return

    if status != "split":
        # Reserve the request and all its download polls before starting a window.
        if client.remaining < 1 + len(POLL_DELAYS):
            raise BackfillPaused("Notebook request budget reached. Rerun to continue.")
        try:
            token = request_export(client, kind, start, end)
        except ExtractTooLarge:
            if start == end:
                raise RuntimeError(f"{kind} {start} exceeds SAM.gov's extract limit even for one day.") from None
            write_json(checkpoint, {"status": "split"})
        else:
            if token is None:
                write_json(checkpoint, {"status": "empty"})
                return

            file = download_export(client, kind, token, run_dir / partition)
            relative = file.relative_to(root).as_posix()
            write_json(checkpoint, {
                "status": "saved",
                "path": relative,
                "bytes": file.stat().st_size,
                "downloaded_utc": datetime.now(timezone.utc).isoformat(),
            })
            print(f"Saved {relative}")
            return

    midpoint = start + (end - start) // 2
    load_window(client, root, state, run_dir, kind, midpoint + timedelta(days=1), end)
    load_window(client, root, state, run_dir, kind, start, midpoint)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ingest():
    if not LAKEHOUSE_FILES.is_dir():
        raise RuntimeError("Attach the intended Lakehouse as the default Lakehouse.")

    api_key = sam_api_key.strip()
    if not api_key:
        raise ValueError("Enter the SAM.gov API key in sam_api_key in the second code cell.")

    request_limit = int(max_api_requests)
    if request_limit < 1 + len(POLL_DELAYS):
        raise ValueError("max_api_requests must be at least 5.")

    now = datetime.now(timezone.utc)
    root = LAKEHOUSE_FILES / "raw" / "sam" / "contract-awards"
    state = root / "_checkpoints"
    start = date.fromisoformat(signed_from.strip() or "1978-10-01")
    end = snapshot_end(state, start, now)
    if start > end:
        raise ValueError("signed_from must be on or before signed_to.")

    run_dir = root / f"ingested_date={now:%Y-%m-%d}" / f"run={now:%Y%m%dT%H%M%SZ}-{uuid4().hex}"
    status = "complete"
    with requests.Session() as session:
        client = SamClient(session, api_key, request_limit)
        try:
            for low, high in month_windows(start, end):
                for kind in ("Award", "IDV"):
                    load_window(client, root, state, run_dir, kind, low, high)
        except BackfillPaused as pause:
            status = "paused"
            print(pause)

    receipt = {
        "status": status,
        "signed_from": str(start),
        "signed_to": str(end),
        "ingested_utc": now.isoformat(),
        "api_requests": request_limit - client.remaining,
    }
    write_json(run_dir / "_RUN.json", receipt)
    if status == "complete":
        write_json(run_dir / "_SUCCESS.json", receipt)
    print(f"Backfill {status}. Run folder: {run_dir.relative_to(LAKEHOUSE_FILES).as_posix()}")


ingest()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
