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
}

NOTEBOOK = json.loads(r'''{
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {},
      "source": [
        "# SAM.gov Award and IDV backfill\n",
        "\n",
        "Prepared by Codex (OpenAI).\n",
        "\n",
        "Attach the destination Lakehouse as the default Lakehouse. In the second code cell, enter the SAM.gov API key in sam_api_key in your private Fabric notebook. Select **Run all**. Keep Git and shared copies free of the key.\n",
        "\n",
        "The default backfill requests public Award and IDV extracts from October 1, 1978 through the UTC date of its first run, newest month first. Set `signed_from` or `signed_to` to use a specific date range. Files land under `Files/raw/sam/contract-awards/ingested_date=YYYY-MM-DD/run=...`.\n",
        "\n",
        "SAM.gov caps each extract and can limit requests per day. A paused run can continue from its saved checkpoints. The `_RUN.json` file shows the status. `_SUCCESS.json` appears once the whole requested backfill has completed.\n",
        "\n",
        "When running through a Fabric pipeline, mark the first code cell as the parameter cell. The pipeline passes the signed-date range.\n"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "tags": [
          "parameters"
        ]
      },
      "outputs": [],
      "source": [
        "# An empty end date uses the UTC date of the first run for this start date.\n",
        "signed_from = \"1978-10-01\"\n",
        "signed_to = \"\"\n",
        "max_api_requests = 10\n"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {},
      "outputs": [],
      "source": [
        "# Paste your SAM.gov API key between the quotes in your private Fabric notebook.\n",
        "sam_api_key = \"\"\n",
        "\n",
        "import json\n",
        "from datetime import date, datetime, timedelta, timezone\n",
        "from pathlib import Path\n",
        "from shutil import copyfileobj\n",
        "from time import sleep\n",
        "from uuid import uuid4\n",
        "\n",
        "import requests\n",
        "\n",
        "API_ROOT = \"https://api.sam.gov/contract-awards/v1\"\n",
        "LAKEHOUSE_FILES = Path(\"/lakehouse/default/Files\")\n",
        "POLL_DELAYS = (30, 90, 180, 300)\n",
        "\n",
        "\n",
        "class BackfillPaused(Exception):\n",
        "    \"\"\"The run can continue later from the saved partitions.\"\"\"\n",
        "\n",
        "\n",
        "class ExtractTooLarge(Exception):\n",
        "    \"\"\"SAM.gov requires a smaller date range.\"\"\"\n",
        "\n",
        "\n",
        "class SamClient:\n",
        "    def __init__(self, session, api_key, request_limit):\n",
        "        self.session = session\n",
        "        self.api_key = api_key\n",
        "        self.remaining = request_limit\n",
        "\n",
        "    def get(self, endpoint, params, *, stream=False):\n",
        "        self.remaining -= 1\n",
        "        try:\n",
        "            response = self.session.get(\n",
        "                f\"{API_ROOT}/{endpoint}\",\n",
        "                params={**params, \"api_key\": self.api_key},\n",
        "                timeout=(15, 120),\n",
        "                stream=stream,\n",
        "                allow_redirects=False,\n",
        "            )\n",
        "        except requests.RequestException:\n",
        "            raise RuntimeError(f\"SAM.gov {endpoint} request failed.\") from None\n",
        "        if response.status_code == 429:\n",
        "            response.close()\n",
        "            raise BackfillPaused(\"SAM.gov rate limit reached. Resume after the limit resets.\")\n",
        "        return response\n",
        "\n",
        "\n",
        "def response_text(response):\n",
        "    try:\n",
        "        return response.text.lower()\n",
        "    except requests.RequestException:\n",
        "        raise RuntimeError(\"Could not read the SAM.gov export status.\") from None\n",
        "\n",
        "\n",
        "def request_export(client, kind, start, end):\n",
        "    params = {\n",
        "        \"awardOrIDV\": kind,\n",
        "        \"dateSigned\": f\"[{start:%m/%d/%Y},{end:%m/%d/%Y}]\",\n",
        "        \"format\": \"csv\",\n",
        "    }\n",
        "    with client.get(\"search\", params) as response:\n",
        "        if response.status_code == 204:\n",
        "            return None\n",
        "        if response.status_code == 400:\n",
        "            error = response_text(response)\n",
        "            if \"count exceeded error\" in error or \"exceeded the maximum allowable limit\" in error:\n",
        "                raise ExtractTooLarge()\n",
        "        if response.status_code not in (200, 202):\n",
        "            raise RuntimeError(f\"{kind} {start} to {end}: export request returned HTTP {response.status_code}.\")\n",
        "        try:\n",
        "            token = response.json().get(\"exportToken\")\n",
        "        except (ValueError, AttributeError):\n",
        "            token = None\n",
        "        if not isinstance(token, str) or not token:\n",
        "            raise RuntimeError(f\"SAM.gov did not return an export token for {kind} {start} to {end}.\")\n",
        "        return token\n",
        "\n",
        "\n",
        "def download_export(client, kind, token, folder):\n",
        "    for delay in POLL_DELAYS:\n",
        "        sleep(delay)\n",
        "        with client.get(\"download\", {\"token\": token}, stream=True) as response:\n",
        "            if response.status_code == 202:\n",
        "                continue\n",
        "            if response.status_code == 400 and \"file is not generated yet\" in response_text(response):\n",
        "                continue\n",
        "            if response.status_code != 200:\n",
        "                raise RuntimeError(f\"{kind} download returned HTTP {response.status_code}.\")\n",
        "\n",
        "            content_type = response.headers.get(\"Content-Type\", \"\").partition(\";\")[0].strip().lower()\n",
        "            if content_type == \"application/json\":\n",
        "                message = response_text(response)\n",
        "                if \"file is not generated yet\" in message or \"generation is in progress\" in message:\n",
        "                    continue\n",
        "                raise RuntimeError(f\"SAM.gov returned a status response instead of the {kind} extract.\")\n",
        "\n",
        "            response.raw.decode_content = True\n",
        "            staged = folder / f\"{kind.lower()}.part\"\n",
        "            try:\n",
        "                prefix = response.raw.read(512)\n",
        "                first_line = prefix.lstrip(b\"\\xef\\xbb\\xbf \\t\\r\\n\").partition(b\"\\n\")[0]\n",
        "                if prefix.startswith((b\"PK\\x03\\x04\", b\"PK\\x05\\x06\")):\n",
        "                    extension = \"zip\"\n",
        "                elif content_type in (\n",
        "                    \"\", \"text/csv\", \"application/csv\", \"text/plain\",\n",
        "                    \"application/octet-stream\", \"application/vnd.ms-excel\",\n",
        "                ) and b\",\" in first_line and not first_line.startswith((b\"{\", b\"<\", b\"[\")):\n",
        "                    extension = \"csv\"\n",
        "                else:\n",
        "                    raise ValueError(\"Unexpected extract format.\")\n",
        "\n",
        "                folder.mkdir(parents=True, exist_ok=True)\n",
        "                with staged.open(\"wb\") as output:\n",
        "                    output.write(prefix)\n",
        "                    copyfileobj(response.raw, output, length=1024 * 1024)\n",
        "                target = folder / f\"{kind.lower()}.{extension}\"\n",
        "                staged.replace(target)\n",
        "                return target\n",
        "            except Exception:\n",
        "                staged.unlink(missing_ok=True)\n",
        "                raise RuntimeError(f\"Could not save a complete CSV or ZIP for {kind}.\") from None\n",
        "    raise BackfillPaused(f\"The {kind} export was still pending. Completed windows are saved.\")\n"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {},
      "outputs": [],
      "source": [
        "def write_json(path, payload):\n",
        "    path.parent.mkdir(parents=True, exist_ok=True)\n",
        "    staged = path.with_name(f\".{path.name}.{uuid4().hex}.part\")\n",
        "    try:\n",
        "        staged.write_text(json.dumps(payload, indent=2), encoding=\"utf-8\")\n",
        "        staged.replace(path)\n",
        "    finally:\n",
        "        staged.unlink(missing_ok=True)\n",
        "\n",
        "\n",
        "def snapshot_end(state, start, now):\n",
        "    if signed_to.strip():\n",
        "        return date.fromisoformat(signed_to.strip())\n",
        "\n",
        "    path = state / f\"snapshot-from-{start}.json\"\n",
        "    if path.exists():\n",
        "        return date.fromisoformat(json.loads(path.read_text(encoding=\"utf-8\"))[\"signed_to\"])\n",
        "    end = now.date()\n",
        "    if end >= start:\n",
        "        write_json(path, {\"signed_to\": str(end)})\n",
        "    return end\n",
        "\n",
        "\n",
        "def month_windows(start, end):\n",
        "    while end >= start:\n",
        "        low = max(start, end.replace(day=1))\n",
        "        yield low, end\n",
        "        end = low - timedelta(days=1)\n",
        "\n",
        "\n",
        "def load_window(client, root, state, run_dir, kind, start, end):\n",
        "    partition = f\"record_type={kind.lower()}/signed_from={start}/signed_to={end}\"\n",
        "    checkpoint = state / partition / \"status.json\"\n",
        "    previous = json.loads(checkpoint.read_text(encoding=\"utf-8\")) if checkpoint.exists() else {}\n",
        "    status = previous.get(\"status\")\n",
        "    if status == \"empty\":\n",
        "        return\n",
        "    if status == \"saved\":\n",
        "        file = root / previous[\"path\"]\n",
        "        if file.is_file() and file.stat().st_size == previous[\"bytes\"]:\n",
        "            return\n",
        "\n",
        "    if status != \"split\":\n",
        "        # Reserve the request and all its download polls before starting a window.\n",
        "        if client.remaining < 1 + len(POLL_DELAYS):\n",
        "            raise BackfillPaused(\"Notebook request budget reached. Rerun to continue.\")\n",
        "        try:\n",
        "            token = request_export(client, kind, start, end)\n",
        "        except ExtractTooLarge:\n",
        "            if start == end:\n",
        "                raise RuntimeError(f\"{kind} {start} exceeds SAM.gov's extract limit even for one day.\") from None\n",
        "            write_json(checkpoint, {\"status\": \"split\"})\n",
        "        else:\n",
        "            if token is None:\n",
        "                write_json(checkpoint, {\"status\": \"empty\"})\n",
        "                return\n",
        "\n",
        "            file = download_export(client, kind, token, run_dir / partition)\n",
        "            relative = file.relative_to(root).as_posix()\n",
        "            write_json(checkpoint, {\n",
        "                \"status\": \"saved\",\n",
        "                \"path\": relative,\n",
        "                \"bytes\": file.stat().st_size,\n",
        "                \"downloaded_utc\": datetime.now(timezone.utc).isoformat(),\n",
        "            })\n",
        "            print(f\"Saved {relative}\")\n",
        "            return\n",
        "\n",
        "    midpoint = start + (end - start) // 2\n",
        "    load_window(client, root, state, run_dir, kind, midpoint + timedelta(days=1), end)\n",
        "    load_window(client, root, state, run_dir, kind, start, midpoint)\n"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {},
      "outputs": [],
      "source": [
        "def ingest():\n",
        "    if not LAKEHOUSE_FILES.is_dir():\n",
        "        raise RuntimeError(\"Attach the intended Lakehouse as the default Lakehouse.\")\n",
        "\n",
        "    api_key = sam_api_key.strip()\n",
        "    if not api_key:\n",
        "        raise ValueError(\"Enter the SAM.gov API key in sam_api_key in the second code cell.\")\n",
        "\n",
        "    request_limit = int(max_api_requests)\n",
        "    if request_limit < 1 + len(POLL_DELAYS):\n",
        "        raise ValueError(\"max_api_requests must be at least 5.\")\n",
        "\n",
        "    now = datetime.now(timezone.utc)\n",
        "    root = LAKEHOUSE_FILES / \"raw\" / \"sam\" / \"contract-awards\"\n",
        "    state = root / \"_checkpoints\"\n",
        "    start = date.fromisoformat(signed_from.strip() or \"1978-10-01\")\n",
        "    end = snapshot_end(state, start, now)\n",
        "    if start > end:\n",
        "        raise ValueError(\"signed_from must be on or before signed_to.\")\n",
        "\n",
        "    run_dir = root / f\"ingested_date={now:%Y-%m-%d}\" / f\"run={now:%Y%m%dT%H%M%SZ}-{uuid4().hex}\"\n",
        "    status = \"complete\"\n",
        "    with requests.Session() as session:\n",
        "        client = SamClient(session, api_key, request_limit)\n",
        "        try:\n",
        "            for low, high in month_windows(start, end):\n",
        "                for kind in (\"Award\", \"IDV\"):\n",
        "                    load_window(client, root, state, run_dir, kind, low, high)\n",
        "        except BackfillPaused as pause:\n",
        "            status = \"paused\"\n",
        "            print(pause)\n",
        "\n",
        "    receipt = {\n",
        "        \"status\": status,\n",
        "        \"signed_from\": str(start),\n",
        "        \"signed_to\": str(end),\n",
        "        \"ingested_utc\": now.isoformat(),\n",
        "        \"api_requests\": request_limit - client.remaining,\n",
        "    }\n",
        "    write_json(run_dir / \"_RUN.json\", receipt)\n",
        "    if status == \"complete\":\n",
        "        write_json(run_dir / \"_SUCCESS.json\", receipt)\n",
        "    print(f\"Backfill {status}. Run folder: {run_dir.relative_to(LAKEHOUSE_FILES).as_posix()}\")\n",
        "\n",
        "\n",
        "ingest()\n"
      ]
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
    },
    "authors": [
      {
        "name": "Codex (OpenAI)"
      }
    ]
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

    existing_parameters = {
        name: value for name, value in properties.get("parameters", {}).items()
        if name not in {"key_vault_url", "secret_name"}
    }
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
    print("Set sam_api_key in the private loader notebook before running the pipeline.")
    print(json.dumps({"name": PIPELINE_NAME, "objectId": pipeline["id"], **deployed}, indent=2))
    return original_definition


original_pipeline_definition = install()
