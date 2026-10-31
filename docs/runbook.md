# Runbook

How to deploy the lakehouse, run it, check it and switch it off. Commands are for PowerShell with the Azure CLI and the Databricks CLI installed.

## Before the first deploy

1. Sign in with `az login` and pick the subscription with `az account set --subscription <id>`.
2. Check the vCPU quota in the region. The job cluster needs four cores of the Ddsv4 family.

   ```
   az vm list-usage --location switzerlandnorth --query "[?contains(name.value, 'DDSv4') || name.value=='cores']" -o table
   ```

3. Register the two resource providers a new subscription won't have yet. Each takes a minute or two.

   ```
   az provider register -n Microsoft.Databricks --wait
   az provider register -n Microsoft.DataFactory --wait
   ```

4. Create a resource group. Everything lives in it, so deleting it removes everything.

   ```
   az group create -n rg-epd-lakehouse -l switzerlandnorth
   ```

## Deploy

**1. Infrastructure.** The first deploy has no job ID yet, which is fine.

```
$env:BUDGET_EMAIL = "you@example.com"
az deployment group create -g rg-epd-lakehouse -f infra/main.bicep -p infra/main.bicepparam --query properties.outputs
```

Keep the outputs. `storageAccount`, `databricksUrl` and `accessConnectorId` are needed below.

**2. Unity Catalog storage.** In the Databricks workspace, under Catalog, then External data, do three things.

- Create a storage credential from the access connector ID. Choose Storage credential, not Service credential, and do this in the workspace rather than through the REST API, which refuses it with a permissions error.
- Create an external location for `abfss://lake@<storageAccount>.dfs.core.windows.net/` and one for `abfss://raw@<storageAccount>.dfs.core.windows.net/`, both using that credential.
- Create the catalog with its managed location in the lake.

  ```sql
  CREATE CATALOG IF NOT EXISTS epd MANAGED LOCATION 'abfss://lake@<storageAccount>.dfs.core.windows.net/epd';
  ```

**3. The Databricks job.**

`databricks.yml` names no workspace, so save the login under the profile name `DEFAULT`. Data Factory's identity must be added to the workspace before the job can name it. Its Azure role alone doesn't let it in.

```
databricks auth login --host <databricksUrl>
$appId = az ad sp show --id <dataFactoryPrincipalId> --query appId -o tsv
databricks service-principals create --application-id $appId --display-name <dataFactoryName>
databricks bundle deploy --var="storage_account=<storageAccount>" --var="data_factory_app_id=$appId"
databricks bundle summary --var="storage_account=<storageAccount>" --var="data_factory_app_id=$appId"
```

The new identity has no entitlements, and Databricks refuses all API calls without one. In the workspace, open Settings, then Identity and access, then Service principals, pick the identity and tick Workspace access. This only lets it call the API. Its rights still come from the job's permission list.

Note the job ID from the summary.

**4. Connect Data Factory to the job.** Redeploy the infrastructure with the job ID. Nothing else changes.

```
$env:DATABRICKS_JOB_ID = "<job id>"
az deployment group create -g rg-epd-lakehouse -f infra/main.bicep -p infra/main.bicepparam
```

## Run

**A backfill.** In Data Factory, trigger `PL_INGEST_EPD` with `windowStart` and `windowEnd` set to the first and last month, for example `202407` and `202506`. It lands only months that aren't already in raw, then runs the job and waits for it.

**Reprocess months** without landing anything, for example after a rule change.

```
databricks bundle run epd_lakehouse --params months=202505,202506
```

**Monthly schedule.** The trigger `TR_MONTHLY` is created stopped. Start it when you want new months loaded automatically, on the 20th of each month.

```
az datafactory trigger start -g rg-epd-lakehouse --factory-name <dataFactoryName> -n TR_MONTHLY
```

## Check

- `epd.ops.reconciliation` has one row per month and check. Every row in a run should have `passed = true`, or the job will have failed.
- `epd.silver.quarantine` holds rejected rows with `_dq_errors` naming the rules they broke.
- The silver task prints a count per broken rule for each run.

## Report

The report is a Power BI Project in `powerbi/`, so the model and every page are text files in Git. It imports `mart_icb_chapter_month`, `dim_icb`, `dim_period` and `ops.reconciliation`, so the warehouse runs only during a refresh and not on every click.

1. Open `powerbi/epd-prescribing.pbip` in Power BI Desktop.
2. If the workspace or warehouse has changed, go to Transform data, then Edit parameters, and set `DatabricksHost` and `WarehousePath`.
3. Choose Refresh. The first time, sign in to Azure Databricks with your Microsoft account.
4. Open the Data checks page. Every check should pass, and both gaps should be zero.

The imported data lives in `.pbi/cache.abf`, which Git ignores, so a fresh clone opens with empty visuals until it is refreshed.

NHSBSA cuts ICB names at 40 characters, so the ICB table's query maps each published name to its full name. A new or renamed ICB falls back to the published wording, so add it to that map.

## Costs and switching off

- The job cluster exists only while the job runs. Nothing runs between loads unless the trigger is started.
- Stop the schedule with `az datafactory trigger stop`.
- Delete everything with `az group delete -n rg-epd-lakehouse`. The Databricks managed resource group goes with the workspace.
- A report refresh needs a Databricks SQL warehouse, which bills while it runs. Use the smallest size with auto-stop after 10 minutes.


