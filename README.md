# SAM.gov contract ingestion for Microsoft Fabric

The pipeline calls one Fabric Notebook activity. The notebook requests an Award CSV extract and an IDV CSV extract from SAM.gov, polls for downloads, and writes the response files to its default Lakehouse under `Files/raw/sam/contract-awards`. A run is complete when both files and `_SUCCESS.json` are present.

## Set up and run

1. In the target Fabric workspace, import `sam-contract-awards.ipynb` as a PySpark notebook and attach the destination Lakehouse as its default. Mark the configuration cell as the parameter cell if Fabric does not retain that designation.
2. Store the SAM.gov API key in Azure Key Vault. Grant the notebook's run identity permission to get the secret and write to the Lakehouse. Do not put the key into the pipeline JSON.
3. Replace the workspace and notebook placeholders with the actual Fabric IDs. In the full editor JSON, replace `<FABRIC_PIPELINE_ID>` with the ID of the existing pipeline. The notebook ID is the ID of the imported notebook, separate from the pipeline object ID.
4. Use `PLN_SAM_GOV_FABRIC.json` for the full editor-shaped definition of the existing pipeline. For a Fabric REST activity definition, use `pipeline-content.json` as the InlineBase64 payload part of the same name. To update the existing item, call its updateDefinition endpoint using the target workspace and pipeline IDs. If required by your deployment process, preserve the existing item's `.platform` part obtained from getDefinition.
5. Start a test run with `signed_from` and `signed_to` as ISO dates, `key_vault_url` as the vault endpoint, and `secret_name` as the Key Vault secret name. Use a small window with known Award and IDV records. Check both files and the completion marker before scheduling. The signed-date range is for bounded pulls and backfills. It does not collect updates to older contracts.

[Fabric pipeline definition](https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/datapipeline-definition) · [Update pipeline definition](https://learn.microsoft.com/rest/api/fabric/datapipeline/items/update-data-pipeline-definition) · [SAM.gov Contract Awards API](https://open.gsa.gov/api/contract-awards/)
