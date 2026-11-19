"""Paste into a Fabric cell after the cell that sets sam_api_key."""

import requests

try:
    with requests.get(
        "https://api.sam.gov/contract-awards/v1/search",
        params={
            "api_key": sam_api_key.strip(),
            "awardOrIDV": "Award",
            "dateSigned": "[11/01/2026,11/19/2026]",
            "limit": 1,
        },
        timeout=30,
        allow_redirects=False,
    ) as response:
        print(f"HTTP {response.status_code}")
except requests.RequestException:
    print("No usable response from the endpoint.")
