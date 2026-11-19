# Run SAM.gov Awards and IDVs from a Fabric pipeline

Run **`PLN_SAM_GOV_FABRIC`** manually in Microsoft Fabric. Its **Notebook** activity runs the PySpark code and lands separate Award and IDV extracts in OneLake.

## The two pieces

| File | Use |
| --- | --- |
| [sam-contract-awards.ipynb](sam-contract-awards.ipynb) | Import this Fabric notebook. [Python source](sam-contract-awards.py) is also available for copy and paste. |
| [PLN_SAM_GOV_FABRIC.json](PLN_SAM_GOV_FABRIC.json) | Full pipeline template with its activity and parameters. [pipeline-content.json](pipeline-content.json) is the same configuration in Fabric REST definition format. |

## Wire it up once in Fabric

1. In the workspace containing `PLN_SAM_GOV_FABRIC`, select **Import > Notebook** and choose `sam-contract-awards.ipynb`. Use PySpark and attach your destination Lakehouse as the notebook's default. If needed, select **Toggle parameter cell** on the first code cell.
2. Open `PLN_SAM_GOV_FABRIC`. Replace the inactive **Set variable1** with a **Notebook** activity named **Download SAM.gov Awards and IDVs**. In **Settings**, select the imported notebook. Selecting it in Fabric binds the notebook and workspace IDs.
3. On the pipeline canvas, create these String pipeline parameters and use the corresponding dynamic expressions under the Notebook activity's **Base parameters**.

| Pipeline and notebook parameter | Notebook activity expression | Run value |
| --- | --- | --- |
| `signed_from` | `@pipeline().parameters.signed_from` | Start date in `YYYY-MM-DD` |
| `signed_to` | `@pipeline().parameters.signed_to` | End date in `YYYY-MM-DD` |
| `key_vault_url` | `@pipeline().parameters.key_vault_url` | The Azure Key Vault endpoint |
| `secret_name` | `@pipeline().parameters.secret_name` | Default `sam-gov-api-key` |

4. Store the API key in the Key Vault secret. The notebook's run identity needs secret-read and Lakehouse write access. Select **Save > Run** from the pipeline and enter the date range and vault URL.

Check **Lakehouse > Files > raw > sam > contract-awards**. Each completed run has separate Award and IDV files and a `_SUCCESS.json` marker. The notebook preserves CSV or ZIP files returned by SAM.gov.

[Fabric Notebook activity](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity) · [Fabric pipeline JSON schema](https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/datapipeline-definition) · [SAM.gov Contract Awards API](https://open.gsa.gov/api/contract-awards/)
