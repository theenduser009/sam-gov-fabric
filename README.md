# SAM.gov Contract Awards and IDVs into Microsoft Fabric

Use this runbook to set up SAM.gov access, extract public contract actions, land the original files in a Fabric Lakehouse, and reconcile a one-year backfill. The source report is the **Data Bank Award/IDV Information Report**. IDV stands for Indefinite Delivery Vehicle.

## 1. Define the load

1. Agree on the inclusive **Date Signed** start and end dates. Use the same dates, record types, agency filters, and report fields when comparing sources. Save the exact filter selections and the required report headers before the first export.
2. Decide the grain with the report owner. An Award or IDV may have a base action and modifications. Keep action identifiers and any referenced IDV identifiers so the team can distinguish those rows.
3. Pick an extraction route for each date window. Use a small matching Data Bank export to check fields, date semantics, and counts before replacing the team's recurring report. Record public disclosure delays when comparing recent data, especially for DoD actions. [SAM.gov Contract Data](https://sam.gov/contract-data) · [GSA public data coverage](https://open.gsa.gov/api/contract-awards/#revealedunrevealed-data)

| Route | When to use it | What lands in Fabric |
| --- | --- | --- |
| SAM.gov Data Bank | Backfill using the existing report and its selected fields | Original whole-report CSV for each signed-date window |
| SAM.gov API paged search | Verify access and ingest smaller ranges with the checked-in notebook | JSON pages with the returned records, checkpoints, and run receipts |
| SAM.gov API asynchronous extract | Automate higher-volume pulls once bulk access and both record types are verified | Original JSON or CSV downloads and a receipt for each completed extract |
| Treasury USAspending | Evaluate an alternative source against a matching SAM.gov report | Original archive or ZIP, with separate Award and IDV data where supplied |

The repository implements **paged JSON extraction and raw file landing**. Data Bank CSVs can be uploaded through Fabric. The asynchronous extract activity and the curated Delta transform described below are implementation steps for a production workflow.

## 2. Create the individual account and API key

1. On **SAM.gov**, select **Sign In**. SAM.gov redirects to Login.gov. Sign in with an existing individual Login.gov account or select **Create an account**. For a new account, confirm the email, create a password, and set up multifactor authentication. [Create a Login.gov account](https://www.login.gov/help/create-account/how-do-i-create-an-account/)
2. Return to SAM.gov from Login.gov and complete the SAM.gov user profile. Open **Workspace** and confirm that signed-in Contract Data and Data Bank reports are available. A SAM.gov user profile and an organization's entity registration are separate workflows. [SAM.gov account setup](https://sam.gov/entity-registration) · [Access to Contract Data](https://sam.gov/contract-data)
3. In the SAM.gov profile, open **Account Details → Public API Keys**. Select **Request API Key** to generate a key for the individual account. Use the eye icon and the emailed one-time code when SAM.gov prompts to reveal it. [GSA individual API key instructions](https://open.gsa.gov/api/contract-awards/#individual-personal-accounts)
4. Enter the active key in `sam_api_key` in the private Fabric copy of [Cell 2](cells/02-api-key-and-search.py). The GitHub template contains an empty placeholder. Keep keys, Login.gov credentials, one-time codes, and download tokens in the approved private location. To rotate a key, use SAM.gov's key management controls, update the private Fabric configuration, and repeat the one-window smoke test. [FSD key management guide](https://www.fsd.gov/gsafsd_sp?id=kb_article_view&sysparm_article=KB0035680)

## 3. Run a paged API ingestion

### Prepare the Fabric notebook

1. Create or open a PySpark notebook in the Fabric workspace. Import [sam-contract-awards.ipynb](sam-contract-awards.ipynb) or copy each of the five linked files into its own code cell. The [Python source](sam-contract-awards.py) contains the complete notebook code.
2. In the notebook's Lakehouse explorer, attach the target Lakehouse and make it the default. The notebook uses `/lakehouse/default/Files`. Fabric exposes this folder under the Lakehouse **Files** node. [Microsoft Lakehouse file access](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-notebook-load-data)
3. Set `signed_from` and `signed_to` in **YYYY-MM-DD** format. Start with a one-day window. Set `max_api_requests` for a small pilot budget, and enter the active key in the private notebook. Keep **Cell 1** designated as the parameter cell for pipeline runs. [Fabric notebook parameters](https://learn.microsoft.com/en-us/fabric/data-engineering/author-execute-notebook#integrate-a-notebook)
4. Run the notebook and look for saved Award and IDV files. Run **Cell 5** on its own to inspect the files already present. It counts saved rows for the selected ingestion date and displays up to five example records per type. It makes zero API calls.

| Cell | Purpose |
| --- | --- |
| [1. Settings](cells/01-settings.py) | Signed-date range and request budget |
| [2. API key and search](cells/02-api-key-and-search.py) | Key placeholder, production endpoint, paging and response handling |
| [3. Save pages and checkpoints](cells/03-save-pages-and-checkpoints.py) | Date windows, file writes, and resume state |
| [4. Run download](cells/04-run-download.py) | Extract Awards and IDVs and write run receipts |
| [5. Preview saved data](cells/05-preview-saved-data.py) | Inspect saved results in an interactive Fabric session |

### Request shape and limits

A redacted paged request looks like this. The notebook passes the parameters through the HTTP client for URL encoding. Run the same request with `awardOrIDV=IDV` for the second record type. [GSA search parameters](https://open.gsa.gov/api/contract-awards/#api-description)

```text
GET https://api.sam.gov/contract-awards/v1/search
api_key=REDACTED
awardOrIDV=Award
dateSigned=[01/15/2025,01/15/2025]
limit=100
```

Omit `format` for paged JSON. Read the page's `awardSummary` array and use `totalRecords` as the reported total for that type and window. `offset` is a page index with a documented default of 0. The notebook omits offset on the first call, advances it on subsequent calls, and checks for repeated pages. At more than **400,000 matching records** for a query, it splits the signed-date window. A single day above that cap needs a narrower filter or a bulk route. [GSA paging rules](https://open.gsa.gov/api/contract-awards/)

| GSA account and key type | Default daily API requests |
| --- | ---: |
| Non-federal individual, no SAM.gov role | 10 |
| Non-federal individual, with a SAM.gov role | 1,000 |
| Federal individual | 1,000 |
| Non-federal system account | 1,000 |
| Federal system account | 10,000 |

GSA allows up to **100 records per search page**. The lowest daily allowance can return at most **1,000 records per day** if every request returns a full page. Plan requests for both record types, including any test calls and download polling. The notebook's **10 calls per run** default is a separate local budget. Increasing it leaves the SAM.gov account quota in place. [GSA API key rate limits](https://open.gsa.gov/api/contract-awards/#api-key-rate-limits)

### Interpret the outcome and resume

| Outcome | What to do |
| --- | --- |
| HTTP 200 with records | Verify the record type, dates, and page count in the Lakehouse. |
| HTTP 204 or zero matches | Account for the completed zero-row window in reconciliation. |
| `The notebook used its request budget` | The run reached its configured cap. Resume with the same signed dates once the account has available calls. |
| HTTP 429 or `SAM.gov rate-limited the request` | The service paused requests. Follow any `Retry-After` guidance and resume after the allowance resets. |
| HTTP 401 or 403 | Verify the active key, production host, and applicable account permissions. Use the response detail to distinguish a missing, invalid, disabled, or unauthorized key. |
| HTTP 400 on search | Check the date range, supported filters, page size, and offset. Capture the sanitized response detail. |
| Count changed or a page repeated | Keep the saved pages and checkpoint for investigation. Reconcile the affected window before publishing its records. |

Run one ingestion at a time per Lakehouse. Checkpoints are shared by record type and signed-date window. Keep the saved data and checkpoints together so a rerun can resume. For a clean rebuild, isolate it in a separate Lakehouse or manage the affected data and checkpoint as a coordinated reset.

Each completed or paused notebook run writes `_RUN.json` with its status, signed-date range, UTC timestamp, and call count. `_SUCCESS.json` marks that paging finished for the requested range against the saved checkpoints. A resumed job may leave its pages across several ingestion dates and run folders. Gather all relevant pages for reconciliation, including earlier files when the final run is short. Unexpected errors can end the notebook before it writes a receipt. Inspect the activity output in that case. The checked-in search code reports the status, so capture the sanitized response detail in a single diagnostic request if FSD needs it.

## 4. Export the full range from SAM.gov Data Bank

1. Sign in and open **Data Bank → Contract Data → Ad Hoc → Award/IDV Information Report**. Open the existing saved report or use **Create → New Report → Report Builder** for a new one. [SAM.gov Ad Hoc Reports](https://sam.gov/reports/awards/adhoc)
2. Select the team's required attributes and metrics. Include **Date Signed**, the contract action identifiers, and a reliable Award/IDV classification. If the report lacks a type column, export each type separately where supported and record the type in the manifest. Compare the headers with the current team report.
3. Filter on one inclusive signed-date window and run the report. Record the filters and the total returned rows. The Ad Hoc report limit is **150,000 rows per report**. Start with a short window. Split windows that hit the limit or show truncation, then confirm that the smaller windows cover every original date. [DoD Data Bank guidance](https://toolbox-piee.eb.mil/site-pages/data-bank)
4. Select **Export → Whole report → CSV file format**. Turn off report title and filter-detail rows for a header-first CSV. Keep the original delimiter, header, and values. Preserve identifiers such as PIID and ZIP code as text, especially when viewing an export in a spreadsheet. [GSA Ad Hoc export guide](https://falextracts.s3.amazonaws.com/Documentation/Contract%20Data/Reports/User%20Reference%20Guide%20-%20Ad%20Hoc%20Reports%20%28beta%29.pdf#page=61)
5. Check the file's parsed row count against the report count before marking the window exported. Upload the originals under the `source=data_bank` folders described below. Record windows with zero matches as complete with a zero count.

## 5. Request system access and test the bulk API

### Set up a system account when required

1. Use the individual SAM.gov profile to open **Workspace → System Accounts**. Follow the FSD guide to request the system account widget or start a system account application. Collect the system purpose, the administrative contacts requested by the form, and the network security information. [FSD system account setup](https://www.fsd.gov/gsafsd_sp?id=kb_article_view&sysparm_article=KB0035680)
2. For public Contract Awards, GSA lists the permission `Contract-Awards: read public`, REST API connection type, and the caller's registered IP addresses. Confirm which outbound addresses Fabric actually uses with the workspace or network owner. If the permission is missing from the form, ask FSD for the approved request path and confirm eligibility for both JSON and CSV extracts. [GSA system account requirements](https://open.gsa.gov/api/contract-awards/#system-accounts)
3. Once approved, set the system account password and retrieve its API key through SAM.gov. Configure the private Fabric notebook or credential mechanism. Test from the same Fabric runtime that will execute the pipeline. A successful paged search verifies search access. Test the extract flow separately to confirm bulk entitlement.

### Submit and download an extract

1. Start with a small known Award and a small known IDV. Use the same search endpoint as in step 3, add `format=json` or `format=csv`, and narrow the query by signed date and a known contract filter. GSA caps an extract at **1,000,000 records per request**. Split over-limit windows into smaller queries. [GSA extract instructions](https://open.gsa.gov/api/contract-awards/#utilizing-the-api-extract)
2. Record the extract submission by record type and window. A successful submission returns a download URL containing a token. Keep that URL or token in private run state. Replace `REPLACE_WITH_API_KEY` with a valid authorized key when calling the returned URL.
3. Retry the download only when the response indicates that the file is still being generated or when retry guidance permits. Track these calls against the quota and keep the existing token while it is usable. An expired token requires a fresh extract request. HTTP 400 can signal several conditions, including invalid filters, a result set above the cap, an expired or pending download, a configured IP mismatch, or insufficient privileges. Record whether the error occurred at **submission** or **download** and use its response detail to diagnose it. [GSA HTTP response codes](https://open.gsa.gov/api/contract-awards/#http-response-codes)
4. Save each downloaded file to Lakehouse **Files** under `source=api_extract`. Check the content and parse the expected records, then verify type, signed dates, and count against the matching Data Bank export. Use the same process for Award and IDV before automating larger windows.

### Submit an FSD case if access is unclear

1. Sign in to the Federal Service Desk and create a SAM incident under the relevant **System Accounts** and **API** category. If an incident already exists, use it to follow up. Ask whether the key type can run the public Award and IDV extract and what permission, approval, or egress IP registration is required. [Federal Service Desk](https://www.fsd.gov/gsafsd_sp) · [GSA support checklist](https://open.gsa.gov/api/contract-awards/#contact-us)
2. Send a text file or ticket body containing the redacted method, endpoint, filters, format, request stage, UTC timestamp, HTTP status, and sanitized response. Include the system account name or ID where applicable and redacted screenshots of the request parameters, relevant headers, and missing permission or error when available. Mask the API key, tokens, cookies, authorization values, and personal details in every attachment.
3. Record the submission, response, entitlement decision, and small Award and IDV test outcomes in the team's private handoff. Use the direct acknowledgement as evidence of the case status. A generic support reply leaves access verification open until the permission is confirmed and a small extract succeeds.

## 6. Land, transform, and reconcile the files

### Land the raw data

Keep the original Data Bank CSVs and bulk downloads. The paged notebook saves each returned record array in a JSON page with its query metadata. Use the UTC ingestion date for storage and the inclusive signed dates to identify the query scope. The notebook writes the first path below. Use the other two for manual CSV and future bulk downloads.

```text
Files/raw/sam/contract-awards/ingested_date=<UTC-date>/run=<id>/source=json_search/
  record_type=<award|idv>/signed_from=<from>/signed_to=<to>/page-0000.json
  _RUN.json
  _SUCCESS.json

Files/raw/sam/contract-awards/ingested_date=<UTC-date>/run=<id>/source=data_bank/
  signed_from=<from>/signed_to=<to>/<export>.csv

Files/raw/sam/contract-awards/ingested_date=<UTC-date>/run=<id>/source=api_extract/
  record_type=<award|idv>/signed_from=<from>/signed_to=<to>/<extract>.json
```

The paged notebook keeps separate resume state under `Files/raw/sam/contract-awards/_checkpoints/json_search/`. In Fabric, use **Lakehouse → Files → Upload files** for manual exports. Record each file's relative path, source, report or request filters, type, date window, source-reported count, parsed row count, and ingestion time. Include the header list, file hash where useful, and any incomplete or zero-row windows. [Fabric Lakehouse explorer](https://learn.microsoft.com/en-us/fabric/data-engineering/navigate-lakehouse-explorer)

### Transform for downstream use

1. Read each CSV with its header and an explicit string schema for identifiers. For the saved paged JSON, read each file and expand the `awardSummary` array to contract action rows. Carry `record_type` from its folder and keep the source path on each row. Treat `totalRecords` as the total for a query window, so it is counted once per window and type.
2. Apply a reviewed field map for JSON paths and Data Bank headers. Map identifiers such as PIID, award agency, modification and transaction numbers, referenced IDV, and vendor identifiers. Parse signed dates and action obligation amounts explicitly. Preserve the original strings in the raw files. Use decimal amounts for money and retain valid negative obligations.
3. Validate record type, required fields, signed-date coverage, and the agreed action key. Preserve distinct modifications and orders. Check for duplicates across runs using that key and retain an exception report for invalid or ambiguous rows. Keep each source visible in the audit trail so overlapping routes can be reconciled before combining them.
4. If the team needs Lakehouse **Tables**, write validated rows to curated Delta tables from a transform notebook. Use a reviewed merge key or replace a validated date partition so rerunning a window is repeatable. For raw delivery, publish the approved files and reconciliation manifest in **Files**. [Fabric Delta tables](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-and-delta-tables) · [Fabric transform tutorial](https://learn.microsoft.com/en-us/fabric/data-engineering/tutorial-lakehouse-data-preparation)

To inspect one uploaded CSV from a notebook attached to the destination Lakehouse, copy its relative **Files** path and substitute it below. Use the full ABFS path when reading from another Lakehouse. [Load Lakehouse data from a notebook](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-notebook-load-data)

```python
csv_path = "Files/raw/sam/contract-awards/<path-to-export>.csv"
records = (
    spark.read.option("header", True)
    .option("inferSchema", False)
    .option("multiLine", True)
    .csv(csv_path)
)
print(f"Parsed rows: {records.count():,}")
display(records.limit(10))
```

### Reconcile before releasing the load

- Confirm every inclusive signed-date window for Award and IDV is represented, including zero-row windows. Check for gaps, overlap, and source limits.
- Compare source report or API totals with parsed counts by window and record type. Account for rejects and duplicates. Match the same filters and extraction time when comparing Data Bank and API counts, and record any visibility or late-reporting difference.
- Validate the required headers, data types, representative contract actions, and date range. Record the raw locations and curated table or file counts in the manifest. Mark the one-year load complete once the team has accepted coverage and counts for both types.

## 7. Run and operate the Fabric pipeline

1. Create or open a Fabric data pipeline. Add the **Notebook** activity **Download SAM.gov Awards and IDVs**. In its Settings, select the workspace and notebook. Verify that the pipeline run identity can execute the notebook and write to the target Lakehouse. [Microsoft Notebook activity](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity)
2. Keep `signed_from` and `signed_to` as pipeline string parameters and pass both into the notebook's matching base parameters. The templates use `@pipeline().parameters.signed_from` and `@pipeline().parameters.signed_to`. Select the Fabric items in the UI to resolve workspace and notebook IDs. For a deployment process that uses JSON directly, populate the placeholders with the IDs from the target environment. [Pipeline template](PLN_SAM_GOV_FABRIC.json) · [Pipeline content](pipeline-content.json)
3. Run a small pilot window and review the Lakehouse files and receipts. A successful Notebook activity can still have a `paused` receipt. Gate downstream transform and full-range completion on saved-file coverage, receipts, and reconciliation. Resume the same window when quota permits. Run one instance against those checkpoints at a time.
4. Finish the backfill with the selected route. For an automated bulk pipeline, implement extract submission, persisted token, delayed status check, file download, validation, and the transform in that order. Retain the latest successful coverage in the manifest.
5. For ongoing ingestion, agree on a refresh cadence and capture updates or newly revealed records with the GSA `publicDeltaDate` filter once that incremental flow is implemented. Track a watermark after successful reconciliation, and validate refreshes using the same action key. The checked-in notebook uses `dateSigned` for the backfill. [GSA query parameters](https://open.gsa.gov/api/contract-awards/#api-description)

Use the USAspending contract archive or its bulk awards endpoint only after comparing record grain, fields, dates, public visibility, and counts against a matching SAM.gov export. Preserve Treasury data under its own source label. [USAspending award data archive](https://www.usaspending.gov/download_center/award_data_archive) · [Treasury bulk download API](https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/bulk_download/awards.md)

## 8. Files to share and where to put them

| What to share | Destination | Include |
| --- | --- | --- |
| Notebook, individual cells, pipeline templates, and this runbook | GitHub | Source files and credential placeholders for repeatable setup |
| Original Data Bank CSVs, API page JSON, and approved bulk downloads | Fabric Lakehouse **Files** | Ingestion date, run, source, record type when available, and signed-date window |
| Reference report or representative export, field map, and required header list | Team's approved handoff location | Report name, filters, record grain, source-to-target fields and data types |
| Run manifest and reconciliation results | Team's approved handoff location | Scope, file paths, source and landed row counts, exceptions, and completion status for Awards and IDVs |
| Sanitized API diagnosis and relevant screenshot | Existing FSD case | Request and response details, UTC timestamp, account type, and the permission or configuration question |
| Validated curated output, when requested | Fabric Lakehouse **Tables** or agreed delivery folder | Validated action rows and the associated reconciliation |

For a team handoff, share the runbook link, the Lakehouse folder for the agreed dates, and the reconciliation manifest. Include the current bulk-access status from the private case record.
