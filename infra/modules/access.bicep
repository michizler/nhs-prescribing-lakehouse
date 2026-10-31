// Role assignments. Data Factory and the Unity Catalog connector can read and write the lake. Data Factory can also
// start the Databricks job through the workspace's API with its own identity.
param storageName string
param workspaceName string
param factoryPrincipalId string
param accessConnectorPrincipalId string

var blobDataContributor = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
var contributor = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'b24988ac-6180-42a0-ab88-20f7382dd24c')

resource account 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageName
}

resource workspace 'Microsoft.Databricks/workspaces@2024-05-01' existing = {
  name: workspaceName
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

// Contributor on the workspace lets Data Factory's managed identity call the Jobs API, the pattern Microsoft
// documents for keyless Data Factory to Databricks calls. It is broader than running one job needs. A tighter
// alternative is to add the identity to the workspace as a service principal with only "Can manage run" on the
// job, at the cost of a manual step in the workspace.
resource factoryOnWorkspace 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(workspace.id, factoryPrincipalId, contributor)
  scope: workspace
  properties: { principalId: factoryPrincipalId, principalType: 'ServicePrincipal', roleDefinitionId: contributor }
}
