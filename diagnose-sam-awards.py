"""Paste into a Fabric cell after the cell that sets sam_api_key."""

import requests

if not sam_api_key.strip():
    raise ValueError("Enter the SAM.gov API key in its notebook cell and run that cell first.")

try:
    with requests.get(
        "https://api.sam.gov/contract-awards/v1/search",
        params={
            "api_key": sam_api_key.strip(),
            "awardOrIDV": "Award",
            "dateSigned": "[06/01/2026,06/05/2026]",
            "limit": 1,
        },
        timeout=30,
        allow_redirects=False,
    ) as response:
        print(f"HTTP {response.status_code}")
        if response.status_code in (401, 403):
            body = response.text.lower()
            if "invalid api key" in body or "invalid api_key" in body:
                print("The response identifies the API key as invalid.")
            elif "disabled api key" in body:
                print("The response identifies the API key as disabled.")
            elif "no api key was supplied" in body or "api key is missing" in body:
                print("The response says it did not receive an API key.")
            elif "text/html" in response.headers.get("Content-Type", "").lower():
                print("The denial was an HTML page. A gateway could be returning it.")
            else:
                print("The response does not identify the reason for the denial.")
except requests.RequestException:
    print("No usable response from the endpoint.")
