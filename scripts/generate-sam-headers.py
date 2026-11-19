"""Build header-only samples from GSA's published JSON-to-CSV mapping."""

import argparse
import csv
import io
import re
from pathlib import Path
from urllib.request import urlopen

import pdfplumber

MAPPING_URL = "https://open.gsa.gov/api/contract-awards/v1/ContractAwardAPI_JSONvsCSV.pdf"


def mappings(document):
    pairs = []
    seen_table_header = False
    with pdfplumber.open(io.BytesIO(document)) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                for left, right in table:
                    if (left, right) == ("Contract Award JSON", "Contract Award CSV"):
                        seen_table_header = True
                        continue
                    if not seen_table_header:
                        continue
                    path = "".join((left or "").split())
                    header = "".join((right or "").split())
                    if not path:
                        raise ValueError(f"A mapping row on PDF page {page.page_number} has no JSON field.")
                    if header:
                        pairs.append((path, header))
                    elif pairs:
                        previous_path, previous_header = pairs[-1]
                        pairs[-1] = (previous_path + path, previous_header)
                    else:
                        raise ValueError("A page continuation appeared before the first mapping.")
    if not seen_table_header or not pairs:
        raise ValueError("The expected GSA mapping table is missing.")
    for path, header in pairs:
        if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.\[\]-]*", path) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.\[\]-]*", header):
            raise ValueError(f"Unexpected field notation in {path!r} or {header!r}.")
    return list(dict.fromkeys(pairs))


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as file:
        csv.writer(file, lineterminator="\n").writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pdf", type=Path, help="Use a local copy of the GSA PDF.")
    parser.add_argument("--output-dir", type=Path, default=Path("samples"))
    args = parser.parse_args()

    if args.pdf:
        document = args.pdf.read_bytes()
    else:
        with urlopen(MAPPING_URL, timeout=30) as response:
            document = response.read()
    if not document.startswith(b"%PDF-"):
        raise ValueError("The published mapping download did not return a PDF.")

    pairs = mappings(document)
    headers = list(dict.fromkeys(header for _, header in pairs))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.output_dir / "sam-award-idv-headers.csv", [headers])
    write_csv(args.output_dir / "sam-award-idv-field-map.csv", [("json_path", "csv_header"), *pairs])
    print(f"Read {len(pairs)} unique mappings and wrote {len(headers)} published CSV headers.")


if __name__ == "__main__":
    main()
