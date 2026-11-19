# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# PARAMETERS CELL ********************

# Set this as the parameter cell when running from a Fabric pipeline.
signed_from = ""
signed_to = ""
key_vault_url = ""
secret_name = "sam-gov-api-key"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import date
from pathlib import Path
from shutil import copyfileobj
from time import sleep
from uuid import uuid4
import json

import requests

API_ROOT = "https://api.sam.gov/contract-awards/v1"
LAKEHOUSE_FILES = Path("/lakehouse/default/Files")
POLL_DELAYS = (30, 60, 120, 180)


def request_sam(session, endpoint, params, stream=False):
    try:
        response = session.get(
            f"{API_ROOT}/{endpoint}",
            params=params,
            timeout=(15, 90),
            stream=stream,
            allow_redirects=False,
        )
    except requests.RequestException:
        raise RuntimeError(f"SAM.gov {endpoint} request failed before a response.") from None
    return response


def request_extract(session, api_key, record_type, signed_range):
    params = {
        "api_key": api_key,
        "awardOrIDV": record_type,
        "dateSigned": signed_range,
        "format": "csv",
    }
    with request_sam(session, "search", params) as response:
        if response.status_code not in (200, 202):
            raise RuntimeError(f"{record_type} extract request returned HTTP {response.status_code}.")
        try:
            token = response.json().get("exportToken")
        except (ValueError, AttributeError):
            token = None
        if not isinstance(token, str) or not token:
            raise RuntimeError(f"SAM.gov did not return an export token for {record_type}.")
        return token


def save_extract(session, api_key, token, record_type, folder):
    params = {"api_key": api_key, "token": token}
    for delay in POLL_DELAYS:
        sleep(delay)
        with request_sam(session, "download", params, stream=True) as response:
            media_type = response.headers.get("Content-Type", "").split(";")[0].strip().lower()
            if response.status_code == 202 or (
                response.status_code == 200 and media_type == "application/json"
            ):
                continue
            if response.status_code == 400:
                # SAM.gov uses HTTP 400 for an extract that is still being generated.
                try:
                    pending = "file is not generated yet" in response.text.lower()
                except requests.RequestException:
                    raise RuntimeError(f"Could not read the {record_type} export status.") from None
                if pending:
                    continue
            if response.status_code != 200:
                raise RuntimeError(f"{record_type} download returned HTTP {response.status_code}.")

            response.raw.decode_content = True
            try:
                prefix = response.raw.read(512)
                if prefix.startswith((b"PK\x03\x04", b"PK\x05\x06")):
                    extension = "zip"
                elif media_type in ("text/csv", "application/csv", "application/octet-stream", "text/plain") and (
                    b"," in prefix.partition(b"\n")[0]
                ) and not prefix.lstrip(b"\xef\xbb\xbf \t\r\n").startswith((b"<", b"{", b"[")):
                    extension = "csv"
                else:
                    raise RuntimeError(f"SAM.gov did not return a CSV or ZIP for {record_type}.")

                target = folder / f"{record_type.lower()}.{extension}"
                with target.open("xb") as output:
                    output.write(prefix)
                    copyfileobj(response.raw, output, length=1024 * 1024)
            except RuntimeError:
                raise
            except Exception:
                raise RuntimeError(f"Could not finish writing the {record_type} extract.") from None

            return {"file": target.name, "bytes": target.stat().st_size}
    raise TimeoutError(f"The {record_type} extract was not ready within the polling budget.")


def ingest():
    start, end = date.fromisoformat(signed_from), date.fromisoformat(signed_to)
    if start > end:
        raise ValueError("signed_from must be on or before signed_to.")
    if not key_vault_url:
        raise ValueError("Set key_vault_url to your Azure Key Vault endpoint.")
    if not LAKEHOUSE_FILES.is_dir():
        raise RuntimeError("Attach the destination Lakehouse as the default Lakehouse.")

    api_key = notebookutils.credentials.getSecret(key_vault_url, secret_name)
    signed_range = f"[{start:%m/%d/%Y},{end:%m/%d/%Y}]"
    folder = LAKEHOUSE_FILES / "raw/sam/contract-awards" / (
        f"signed_from={start}/signed_to={end}/run={uuid4().hex}"
    )
    folder.mkdir(parents=True, exist_ok=False)
    with requests.Session() as session:
        tokens = {
            kind: request_extract(session, api_key, kind, signed_range)
            for kind in ("Award", "IDV")
        }
        files = {
            kind: save_extract(session, api_key, token, kind, folder)
            for kind, token in tokens.items()
        }
    manifest = {"signed_from": str(start), "signed_to": str(end), "files": files}
    (folder / "_SUCCESS.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Saved both extracts under {folder.relative_to(LAKEHOUSE_FILES)}")


ingest()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
