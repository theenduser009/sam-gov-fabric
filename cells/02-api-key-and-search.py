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
