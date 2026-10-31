// Role assignments. Data Factory and the Unity Catalog connector can read and write the lake.
param storageName string
param factoryPrincipalId string
param accessConnectorName string
param accessConnectorPrincipalId string

var blobDataContributor = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
var reader = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'acdd72a7-3385-48ef-bd42-f606fba81ae7')

resource account 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageName
}

resource connector 'Microsoft.Databricks/accessConnectors@2024-05-01' existing = {
  name: accessConnectorName
}

resource factoryOnStorage 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(account.id, factoryPrincipalId, blobDataContributor)
  scope: account
  properties: { principalId: factoryPrincipalId, principalType: 'ServicePrincipal', roleDefinitionId: blobDataContributor }
}

resource connectorOnStorage 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(account.id, accessConnectorPrincipalId, blobDataContributor)
  scope: account
  properties: { principalId: accessConnectorPrincipalId, principalType: 'ServicePrincipal', roleDefinitionId: blobDataContributor }
}

// Unity Catalog won't register a storage credential unless the connector's identity can read the connector itself.
resource connectorOnItself 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(connector.id, accessConnectorPrincipalId, reader)
  scope: connector
  properties: { principalId: accessConnectorPrincipalId, principalType: 'ServicePrincipal', roleDefinitionId: reader }
}

// No Azure role lets Data Factory start the job. Its identity is a service principal in the workspace, with Workspace
// access and "Can manage run" on this one job, set up in docs/runbook.md and databricks.yml.
