# SAM.gov Contract Awards into Microsoft Fabric

This repository contains a PySpark notebook and a Fabric pipeline template for downloading public Contract Awards and Indefinite Delivery Vehicle (IDV) records. The notebook lands raw JSON in the default Lakehouse under `Files/raw/sam/contract-awards/`.

## 1. Run the notebook

1. Import the [Fabric notebook](sam-contract-awards.ipynb) or copy the [individual code cells](cells/) into a PySpark notebook. Attach the target Lakehouse as the default. [Microsoft Lakehouse file access](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-notebook-load-data)
2. Set the inclusive signed-date range in [Cell 1](cells/01-settings.py) and enter the active SAM.gov key in [Cell 2](cells/02-api-key-and-search.py) in the private Fabric notebook.
3. Select **Run all**. The notebook saves each page with the ingestion date, run, record type, and signed-date range. Run [Cell 5](cells/05-preview-saved-data.py) by itself to count and preview files already in the Lakehouse.
4. Keep the same signed dates when resuming. The notebook records the next page by record type and window. `_RUN.json` reports run status. `_SUCCESS.json` marks completion of paging for both types in the requested range.

## 2. Request and interpret the API data

1. Send `GET /contract-awards/v1/search` to `https://api.sam.gov` with an authorized `api_key`. Use `awardOrIDV=Award` or `awardOrIDV=IDV`, a `dateSigned` range in the form `[MM/DD/YYYY,MM/DD/YYYY]`, and `limit=100`. Omit `format` for synchronous JSON search. Follow the `offset` page index after the first page. `totalRecords` applies to the requested type and date window. The search cap is 400,000 rows per query. [GSA Contract Awards API](https://open.gsa.gov/api/contract-awards/)
2. The repository notebook defaults to ten API calls per run. A message about the notebook's request budget indicates that this local cap was reached. HTTP 429 means SAM.gov has rate-limited the request. Use the service's retry guidance when available. [GSA daily quotas](https://open.gsa.gov/api/contract-awards/#api-key-rate-limits)
3. To request an asynchronous extract, add `format=json` or `format=csv` to the search request. A successful request returns a download URL containing a token. Replace `REPLACE_WITH_API_KEY` in that URL with an authorized key and download the file when it is ready. Validate a small extract for each record type before using the bulk route for a backfill. For a system account, check GSA's documented public Contract Awards permission and outbound IP requirements. [GSA extract instructions](https://open.gsa.gov/api/contract-awards/#utilizing-the-api-extract) · [GSA system account requirements](https://open.gsa.gov/api/contract-awards/#system-accounts)
4. Account for the 1-million-record extract limit by splitting requests into smaller signed-date windows. Decode the actual HTTP 400 response before acting on it. GSA lists several causes, including insufficient extract privileges and a result set above the cap. [GSA response codes](https://open.gsa.gov/api/contract-awards/#http-response-codes)

## 3. Complete and verify a backfill

1. For CSV exports from SAM.gov, open **Data Bank → Contract Data → Ad Hoc → Award/IDV Information Report**. Select the required fields and signed-date filters. Choose **Whole report → CSV file format**. Split any window that reaches the Data Bank report limit of 150,000 rows. [SAM.gov Ad Hoc Reports](https://sam.gov/reports/awards/adhoc) · [FSD export instructions](https://www.fsd.gov/gsafsd_sp?id=kb_article_view&sysparm_article=KB0029879)
2. Upload each original CSV to the Lakehouse's **Files** area under a source, ingestion date, and signed-date window. Preserve contract identifiers as strings. Reconcile report counts to the files for each window, verify the required columns and record types, and check that the signed dates cover the full requested range. [Fabric Lakehouse explorer](https://learn.microsoft.com/en-us/fabric/data-engineering/navigate-lakehouse-explorer)
3. To run the notebook from a pipeline, select it in the **Download SAM.gov Awards and IDVs** Notebook activity and pass `signed_from` and `signed_to`. The [pipeline template](PLN_SAM_GOV_FABRIC.json) and [pipeline content](pipeline-content.json) contain binding placeholders. Verify the notebook's completion receipt and reconcile the output before marking a backfill complete. [Microsoft Fabric Notebook activity](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity)
