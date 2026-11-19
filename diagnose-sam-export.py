"""Run after the cell that sets sam_api_key. Requests one CSV export."""

import re
from urllib.parse import unquote

import requests

api_key = sam_api_key.strip()
if not api_key:
    raise ValueError("Run the cell that sets sam_api_key first.")


def safe_error_text(value):
    # Decode any URL escaping before removing the key and query links.
    value = unquote(unquote(value))
    value = re.sub(re.escape(api_key), "[redacted]", value, flags=re.IGNORECASE)
    value = re.sub(r"https?://\S+", "[link removed]", value, flags=re.IGNORECASE)
    return " ".join(value.split())[:400]


try:
    with requests.get(
        "https://api.sam.gov/contract-awards/v1/search",
        params={
            "api_key": api_key,
            "awardOrIDV": "Award",
            "dateSigned": "[11/01/2026,11/19/2026]",
            "format": "csv",
        },
        timeout=30,
        allow_redirects=False,
    ) as response:
        print(f"Award CSV export HTTP {response.status_code}")
        if response.status_code in (200, 202):
            print("SAM.gov accepted the CSV export request.")
        elif response.status_code == 204:
            print("SAM.gov returned no data for the requested dates.")
        else:
            try:
                error = response.json()
            except ValueError:
                error = None

            messages = []
            if isinstance(error, dict):
                for field in ("message", "detail", "details", "error", "title"):
                    if isinstance(error.get(field), str):
                        message = safe_error_text(error[field])
                        if message and message not in messages:
                            messages.append(message)

            if messages:
                print("\n".join(messages))
            else:
                print("The response had no structured error message.")
except requests.RequestException:
    print("No usable response from the endpoint.")
