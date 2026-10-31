// Parameters for a deployment

param prefix = 'epdlake'
param budgetAmount = 25
param budgetContactEmail = readEnvironmentVariable('BUDGET_EMAIL')
param databricksJobId = readEnvironmentVariable('DATABRICKS_JOB_ID', '')
