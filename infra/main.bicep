// Everything the lakehouse needs in one resource group: storage, Databricks, Data Factory, identities and a budget.
// Deploy with: az deployment group create -g <rg> -f infra/main.bicep -p infra/main.bicepparam
targetScope = 'resourceGroup'

@description('Short lowercase prefix for resource names.')
@minLength(3)
@maxLength(10)
param prefix string = 'epdlake'

param location string = resourceGroup().location

@description('Databricks job ID, known after `databricks bundle deploy`. Redeploy with it so Data Factory can start the job.')
param databricksJobId string = ''

@description('Monthly budget in the subscription currency. Alerts fire at 50%, 80% and 100% of it.')
param budgetAmount int = 25
param budgetContactEmail string

@description('First day of the current month, when the budget starts counting.')
param budgetStartDate string = '${utcNow('yyyy-MM')}-01'

var suffix = take(uniqueString(resourceGroup().id), 6)
var storageName = toLower('${prefix}${suffix}')

module storage 'modules/storage.bicep' = {
  name: 'storage'
  params: { name: storageName, location: location }
}

module databricks 'modules/databricks.bicep' = {
  name: 'databricks'
  params: { name: '${prefix}-dbw-${suffix}', location: location }
}

module factory 'modules/datafactory.bicep' = {
  name: 'datafactory'
  params: {
    name: '${prefix}-adf-${suffix}'
    location: location
    storageDfsUrl: storage.outputs.dfsEndpoint
    databricksWorkspaceUrl: databricks.outputs.workspaceUrl
    databricksJobId: databricksJobId
  }
}

module access 'modules/access.bicep' = {
  name: 'access'
  params: {
    storageName: storageName
    workspaceName: '${prefix}-dbw-${suffix}'
    factoryPrincipalId: factory.outputs.principalId
    accessConnectorPrincipalId: databricks.outputs.accessConnectorPrincipalId
  }
}

module budget 'modules/budget.bicep' = {
  name: 'budget'
  params: { amount: budgetAmount, contactEmail: budgetContactEmail, startDate: budgetStartDate }
}

output storageAccount string = storageName
output rawPath string = 'abfss://raw@${storageName}.dfs.${environment().suffixes.storage}/epd'
output lakePath string = 'abfss://lake@${storageName}.dfs.${environment().suffixes.storage}'
output databricksUrl string = 'https://${databricks.outputs.workspaceUrl}'
output accessConnectorId string = databricks.outputs.accessConnectorId
output dataFactoryName string = factory.outputs.name
output dataFactoryPrincipalId string = factory.outputs.principalId
