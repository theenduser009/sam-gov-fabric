# Paste this entire file into one PySpark cell in the target Fabric workspace.
# Attach the destination Lakehouse as the default before running the cell.
import base64
import json

from sempy.fabric import FabricRestClient

PIPELINE_NAME = "PLN_SAM_GOV_FABRIC"
PIPELINE_ID = None
NOTEBOOK_NAME = "NB_SAM_GOV_CONTRACT_AWARDS"
ACTIVITY_NAME = "Download SAM.gov Awards and IDVs"
PARAMETERS = {
    "signed_from": {"type": "String"},
    "signed_to": {"type": "String"},
    "key_vault_url": {"type": "String"},
    "secret_name": {"type": "String", "defaultValue": "sam-gov-api-key"},
}

NOTEBOOK = json.loads(r'''{
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {},
      "source": [
        "# SAM.gov Awards and IDVs → Fabric Lakehouse\n",
        "\n",
        "This notebook is called by `PLN_SAM_GOV_FABRIC`. Run the pipeline with a signed-date range and the Azure Key Vault URL. The Key Vault secret defaults to `sam-gov-api-key`. Both extracts land under `Files/raw/sam/contract-awards` in the default Lakehouse. A `_SUCCESS.json` marker identifies a completed run.\n"
      ]
    },
    {
      "cell_type": "code",
      "metadata": {
        "tags": [
          "parameters"
        ]
      },
      "source": [
        "# Set this as the parameter cell when running from a Fabric pipeline.\n",
        "signed_from = \"\"\n",
        "signed_to = \"\"\n",
        "key_vault_url = \"\"\n",
        "secret_name = \"sam-gov-api-key\"\n"
      ],
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "metadata": {},
      "source": [
        "from datetime import date\n",
        "from pathlib import Path\n",
        "from shutil import copyfileobj\n",
        "from time import sleep\n",
        "from uuid import uuid4\n",
        "import json\n",
        "\n",
        "import requests\n",
        "\n",
        "API_ROOT = \"https://api.sam.gov/contract-awards/v1\"\n",
        "LAKEHOUSE_FILES = Path(\"/lakehouse/default/Files\")\n",
        "POLL_DELAYS = (30, 60, 120, 180)\n",
        "\n",
        "\n",
        "def request_sam(session, endpoint, params, stream=False):\n",
        "    try:\n",
        "        response = session.get(\n",
        "            f\"{API_ROOT}/{endpoint}\",\n",
        "            params=params,\n",
        "            timeout=(15, 90),\n",
        "            stream=stream,\n",
        "            allow_redirects=False,\n",
        "        )\n",
        "    except requests.RequestException:\n",
        "        raise RuntimeError(f\"SAM.gov {endpoint} request failed before a response.\") from None\n",
        "    return response\n",
        "\n",
        "\n",
        "def request_extract(session, api_key, record_type, signed_range):\n",
        "    params = {\n",
        "        \"api_key\": api_key,\n",
        "        \"awardOrIDV\": record_type,\n",
        "        \"dateSigned\": signed_range,\n",
        "        \"format\": \"csv\",\n",
        "    }\n",
        "    with request_sam(session, \"search\", params) as response:\n",
        "        if response.status_code not in (200, 202):\n",
        "            raise RuntimeError(f\"{record_type} extract request returned HTTP {response.status_code}.\")\n",
        "        try:\n",
        "            token = response.json().get(\"exportToken\")\n",
        "        except (ValueError, AttributeError):\n",
        "            token = None\n",
        "        if not isinstance(token, str) or not token:\n",
        "            raise RuntimeError(f\"SAM.gov did not return an export token for {record_type}.\")\n",
        "        return token\n",
        "\n",
        "\n",
        "def save_extract(session, api_key, token, record_type, folder):\n",
        "    params = {\"api_key\": api_key, \"token\": token}\n",
        "    for delay in POLL_DELAYS:\n",
        "        sleep(delay)\n",
        "        with request_sam(session, \"download\", params, stream=True) as response:\n",
        "            media_type = response.headers.get(\"Content-Type\", \"\").split(\";\")[0].strip().lower()\n",
        "            if response.status_code == 202 or (\n",
        "                response.status_code == 200 and media_type == \"application/json\"\n",
        "            ):\n",
        "                continue\n",
        "            if response.status_code != 200:\n",
        "                raise RuntimeError(f\"{record_type} download returned HTTP {response.status_code}.\")\n",
        "\n",
        "            response.raw.decode_content = True\n",
        "            try:\n",
        "                prefix = response.raw.read(512)\n",
        "                if prefix.startswith((b\"PK\\x03\\x04\", b\"PK\\x05\\x06\")):\n",
        "                    extension = \"zip\"\n",
        "                elif media_type in (\"text/csv\", \"application/csv\", \"application/octet-stream\", \"text/plain\") and (\n",
        "                    b\",\" in prefix.partition(b\"\\n\")[0]\n",
        "                ) and not prefix.lstrip(b\"\\xef\\xbb\\xbf \\t\\r\\n\").startswith((b\"<\", b\"{\", b\"[\")):\n",
        "                    extension = \"csv\"\n",
        "                else:\n",
        "                    raise RuntimeError(f\"SAM.gov did not return a CSV or ZIP for {record_type}.\")\n",
        "\n",
        "                target = folder / f\"{record_type.lower()}.{extension}\"\n",
        "                with target.open(\"xb\") as output:\n",
        "                    output.write(prefix)\n",
        "                    copyfileobj(response.raw, output, length=1024 * 1024)\n",
        "            except RuntimeError:\n",
        "                raise\n",
        "            except Exception:\n",
        "                raise RuntimeError(f\"Could not finish writing the {record_type} extract.\") from None\n",
        "\n",
        "            return {\"file\": target.name, \"bytes\": target.stat().st_size}\n",
        "    raise TimeoutError(f\"The {record_type} extract was not ready within the polling budget.\")\n",
        "\n",
        "\n",
        "def ingest():\n",
        "    start, end = date.fromisoformat(signed_from), date.fromisoformat(signed_to)\n",
        "    if start > end:\n",
        "        raise ValueError(\"signed_from must be on or before signed_to.\")\n",
        "    if not key_vault_url:\n",
        "        raise ValueError(\"Set key_vault_url to your Azure Key Vault endpoint.\")\n",
        "    if not LAKEHOUSE_FILES.is_dir():\n",
        "        raise RuntimeError(\"Attach the destination Lakehouse as the default Lakehouse.\")\n",
        "\n",
        "    api_key = notebookutils.credentials.getSecret(key_vault_url, secret_name)\n",
        "    signed_range = f\"[{start:%m/%d/%Y},{end:%m/%d/%Y}]\"\n",
        "    folder = LAKEHOUSE_FILES / \"raw/sam/contract-awards\" / (\n",
        "        f\"signed_from={start}/signed_to={end}/run={uuid4().hex}\"\n",
        "    )\n",
        "    folder.mkdir(parents=True, exist_ok=False)\n",
        "    with requests.Session() as session:\n",
        "        tokens = {\n",
        "            kind: request_extract(session, api_key, kind, signed_range)\n",
        "            for kind in (\"Award\", \"IDV\")\n",
        "        }\n",
        "        files = {\n",
        "            kind: save_extract(session, api_key, token, kind, folder)\n",
        "            for kind, token in tokens.items()\n",
        "        }\n",
        "    manifest = {\"signed_from\": str(start), \"signed_to\": str(end), \"files\": files}\n",
        "    (folder / \"_SUCCESS.json\").write_text(json.dumps(manifest, indent=2), encoding=\"utf-8\")\n",
        "    print(f\"Saved both extracts under {folder.relative_to(LAKEHOUSE_FILES)}\")\n",
        "\n",
        "\n",
        "ingest()\n"
      ],
      "execution_count": null,
      "outputs": []
    }
  ],
  "metadata": {
    "kernelspec": {
      "display_name": "PySpark",
      "language": "python",
      "name": "synapse_pyspark"
    },
    "language_info": {
      "name": "python"
    }
  },
  "nbformat": 4,
  "nbformat_minor": 4
}''')

def fabric_request(client, method, path, **kwargs):
    response = client.request(method, path, timeout=60, **kwargs)
    response.raise_for_status()
    return response


def find_item(client, workspace, item_type, name):
    path = f"{workspace}/items?type={item_type}"
    matches = []
    while path:
        page = fabric_request(client, "GET", path).json()
        matches.extend(item for item in page["value"] if item["displayName"] == name)
        path = page.get("continuationUri")
    if len(matches) > 1:
        raise RuntimeError(f"Multiple {item_type} items named {name} exist in this workspace.")
    return matches[0] if matches else None


def get_definition(client, pipeline_path):
    response = fabric_request(client, "POST", f"{pipeline_path}/getDefinition", lro_wait=True)
    definition = response.json()["definition"]
    part = next(part for part in definition["parts"] if part["path"] == "pipeline-content.json")
    return definition, json.loads(base64.b64decode(part["payload"]))


def notebook_activity(workspace_id, notebook_id):
    return {
        "name": ACTIVITY_NAME,
        "type": "TridentNotebook",
        "dependsOn": [],
        "policy": {
            "timeout": "0.00:40:00",
            "retry": 0,
            "retryIntervalInSeconds": 30,
            "secureInput": True,
            "secureOutput": True,
        },
        "typeProperties": {
            "notebookId": notebook_id,
            "workspaceId": workspace_id,
            "parameters": {
                name: {
                    "value": {"value": f"@pipeline().parameters.{name}", "type": "Expression"},
                    "type": "string",
                }
                for name in PARAMETERS
            },
        },
    }


def install():
    context = notebookutils.runtime.context
    workspace_id = context["currentWorkspaceId"]
    lakehouse_name = context["defaultLakehouseName"]
    lakehouse_id = context["defaultLakehouseId"]
    if not lakehouse_name or not lakehouse_id:
        raise RuntimeError("Attach the destination Lakehouse as this notebook's default and rerun.")
    lakehouse_workspace_id = context["defaultLakehouseWorkspaceId"] or workspace_id
    workspace = f"/v1/workspaces/{workspace_id}"
    client = FabricRestClient()

    if PIPELINE_ID:
        pipeline = fabric_request(client, "GET", f"{workspace}/items/{PIPELINE_ID}").json()
    else:
        pipeline = find_item(client, workspace, "DataPipeline", PIPELINE_NAME)
    if not pipeline or pipeline["type"] != "DataPipeline" or pipeline["displayName"] != PIPELINE_NAME:
        raise RuntimeError(f"Open a notebook in the workspace containing {PIPELINE_NAME}.")
    pipeline_path = f'{workspace}/dataPipelines/{pipeline["id"]}'

    original_definition, original = get_definition(client, pipeline_path)
    properties = original["properties"]
    activities = properties["activities"]
    activity = activities[0] if len(activities) == 1 else {}
    draft = not activities or (
        activity.get("name") == "Set variable1"
        and activity.get("type") == "SetVariable"
        and activity.get("state") == "Inactive"
        and activity.get("typeProperties", {}).get("variableName") == "TT"
    )
    if not draft and (activity.get("name"), activity.get("type")) != (ACTIVITY_NAME, "TridentNotebook"):
        raise RuntimeError("The live pipeline has different activities. Its definition was not overwritten.")

    existing_parameters = properties.get("parameters", {})
    for name, config in PARAMETERS.items():
        if name in existing_parameters and existing_parameters[name]["type"].lower() != config["type"].lower():
            raise RuntimeError(f"The existing parameter {name} has a different type.")

    description = f"SAM.gov Award and IDV extract loader (Lakehouse {lakehouse_id})."
    installed_notebook = find_item(client, workspace, "Notebook", NOTEBOOK_NAME)
    if installed_notebook and installed_notebook.get("description") != description:
        raise RuntimeError(f"{NOTEBOOK_NAME} already exists with a different Lakehouse or description.")
    if installed_notebook:
        notebook_id = installed_notebook["id"]
    else:
        artifact = notebookutils.notebook.create(
            name=NOTEBOOK_NAME,
            description=description,
            content=json.dumps(NOTEBOOK),
            defaultLakehouse=lakehouse_name,
            defaultLakehouseWorkspace=lakehouse_workspace_id,
            workspaceId=workspace_id,
        )
        notebook_id = str(artifact.id)

    expected = notebook_activity(workspace_id, notebook_id)
    updated = {
        **original,
        "properties": {
            **properties,
            "parameters": {**PARAMETERS, **existing_parameters},
            "activities": [expected],
        },
    }
    if updated != original:
        _, latest = get_definition(client, pipeline_path)
        if latest != original:
            raise RuntimeError("The pipeline changed during setup. Rerun after the other edit is saved.")
        payload = base64.b64encode(json.dumps(updated).encode("utf-8")).decode("ascii")
        parts = [
            {**part, "payload": payload} if part["path"] == "pipeline-content.json" else part
            for part in original_definition["parts"]
        ]
        fabric_request(
            client,
            "POST",
            f"{pipeline_path}/updateDefinition",
            json={"definition": {**original_definition, "parts": parts}},
            lro_wait=True,
        )

    _, deployed = get_definition(client, pipeline_path)
    deployed_activities = deployed["properties"]["activities"]
    if len(deployed_activities) != 1:
        raise RuntimeError("Fabric did not return the expected notebook activity.")
    actual = deployed_activities[0]
    actual_settings = actual["typeProperties"]
    if (
        actual["name"] != ACTIVITY_NAME
        or actual["type"] != "TridentNotebook"
        or actual_settings["notebookId"] != notebook_id
        or actual_settings["workspaceId"] != workspace_id
        or actual_settings["parameters"] != expected["typeProperties"]["parameters"]
    ):
        raise RuntimeError("Fabric did not return the expected notebook binding or parameters.")

    print(f"Installed and verified {PIPELINE_NAME}. Reload it in Fabric and choose Run.")
    print("Provide signed_from, signed_to, and key_vault_url. The API key stays in Key Vault.")
    print(json.dumps({"name": PIPELINE_NAME, "objectId": pipeline["id"], **deployed}, indent=2))
    return original_definition


original_pipeline_definition = install()
