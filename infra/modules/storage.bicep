// ADLS Gen2 account with two containers: raw (files as landed) and lake (Delta tables and checkpoints).
// No public access and no account keys: every reader and writer uses a managed identity.
param name string
param location string

resource account 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: name
  location: location
  kind: 'StorageV2'
  sku: { name: 'Standard_LRS' }
  properties: {
    isHnsEnabled: true                 // hierarchical namespace: real directories, atomic renames for Delta
    accessTier: 'Hot'
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
  }
}

resource blob 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: account
  name: 'default'
}

resource raw 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blob
  name: 'raw'
}

resource lake 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blob
  name: 'lake'
}

// Landed files are read once by Auto Loader and then only on a rebuild, so they move to the cheaper cool tier
// after 30 days. They are kept, because they are the evidence for every number downstream.
resource lifecycle 'Microsoft.Storage/storageAccounts/managementPolicies@2023-05-01' = {
  parent: account
  name: 'default'
  properties: {
    policy: {
      rules: [
        {
          name: 'raw-to-cool'
          enabled: true
          type: 'Lifecycle'
          definition: {
            filters: { blobTypes: [ 'blockBlob' ], prefixMatch: [ 'raw/' ] }
            actions: { baseBlob: { tierToCool: { daysAfterModificationGreaterThan: 30 } } }
          }
        }
      ]
    }
  }
}

output id string = account.id
output dfsEndpoint string = account.properties.primaryEndpoints.dfs
