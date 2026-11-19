# Load SAM.gov Awards and IDVs into Fabric

The PySpark notebook lands separate Award and IDV extracts in the default Lakehouse. Run it directly in Fabric or from the Notebook activity in `PLN_SAM_GOV_FABRIC`.

## Run the notebook directly

1. Import [sam-contract-awards.ipynb](sam-contract-awards.ipynb) as a PySpark notebook, or copy the four cells from the [Python source](sam-contract-awards.py). Attach the destination Lakehouse as the default.
2. In the second code cell of your private Fabric notebook, paste the SAM.gov API key between the quotes on the sam_api_key line. Keep Git and shared copies free of the key.
3. Select **Run all**. The default range starts October 1, 1978 and ends on the UTC date of the first run. The notebook processes the newest month first. Set `signed_from` and `signed_to` to use a specific inclusive range.

The notebook saves the original CSV or ZIP extracts under this hierarchy.

    Files/raw/sam/contract-awards/
      ingested_date=YYYY-MM-DD/
        run=YYYYMMDDTHHMMSSZ-<run-id>/
          record_type=award/signed_from=YYYY-MM-DD/signed_to=YYYY-MM-DD/award.csv
          record_type=idv/signed_from=YYYY-MM-DD/signed_to=YYYY-MM-DD/idv.csv
          _RUN.json

Rerun with the same date range to resume. Completed and empty windows are tracked in `_checkpoints`. Oversized extracts are split into smaller signed-date windows. The notebook defaults to 10 API requests per run. Resume after the account's allowance resets if SAM.gov rate-limits the run. A paused run preserves saved files. `_SUCCESS.json` appears only once the requested date range is complete for both types.

## Run from a Fabric pipeline

[PLN_SAM_GOV_FABRIC.json](PLN_SAM_GOV_FABRIC.json) is the full pipeline template. [pipeline-content.json](pipeline-content.json) is the same configuration in Fabric REST definition format.

1. In the workspace containing `PLN_SAM_GOV_FABRIC`, import the notebook and attach the default Lakehouse. If needed, select **Toggle parameter cell** on the first code cell.
2. Open the pipeline. Replace the inactive **Set variable1** with a **Notebook** activity named **Download SAM.gov Awards and IDVs**. In **Settings**, select the imported notebook to bind its IDs.
3. On the pipeline canvas, create these String pipeline parameters and use the corresponding dynamic expressions under the Notebook activity's **Base parameters**.

| Pipeline and notebook parameter | Notebook activity expression | Run value |
| --- | --- | --- |
| `signed_from` | `@pipeline().parameters.signed_from` | Start date in `YYYY-MM-DD` |
| `signed_to` | `@pipeline().parameters.signed_to` | End date in `YYYY-MM-DD` |

4. Enter the SAM.gov key in the notebook before selecting **Save > Run** in the pipeline. The run identity needs Lakehouse write access. Enter the signed-date range at run time.

[Fabric Notebook activity](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity) · [Fabric pipeline JSON schema](https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/datapipeline-definition) · [SAM.gov Contract Awards API](https://open.gsa.gov/api/contract-awards/)
