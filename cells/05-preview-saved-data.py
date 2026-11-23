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
