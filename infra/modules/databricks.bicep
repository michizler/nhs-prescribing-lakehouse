// Premium Databricks workspace (Unity Catalog needs Premium) and an access connector, the managed identity Unity
// Catalog uses to reach the lake storage without keys.
param name string
param location string

resource connector 'Microsoft.Databricks/accessConnectors@2024-05-01' = {
  name: '${name}-ac'
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {}
}

resource workspace 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: name
  location: location
  sku: { name: 'premium' }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', '${name}-managed')
  }
}

output workspaceUrl string = workspace.properties.workspaceUrl
output workspaceId string = workspace.id
output accessConnectorId string = connector.id
output accessConnectorPrincipalId string = connector.identity.principalId
