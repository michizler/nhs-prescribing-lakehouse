// Data Factory: lands each month of the English Prescribing Dataset in the lake, then runs the Databricks job and
// waits for it. The list of months comes from the publisher's own catalogue API, so a newly published month is
// picked up without any configuration change.
param name string
param location string
param storageDfsUrl string
param databricksWorkspaceUrl string
param databricksJobId string

@description('First run of the monthly schedule. NHSBSA publishes each month about two months after it ends.')
param scheduleStart string = '2026-11-20T06:00:00Z'

resource factory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: name
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {}
}

// ---------- connections ----------

resource nhsbsa 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: factory
  name: 'LS_NHSBSA_OPEN_DATA'
  properties: {
    type: 'HttpServer'
    description: 'NHSBSA open data portal. Public, so anonymous.'
    typeProperties: { url: 'https://opendata.nhsbsa.net/', authenticationType: 'Anonymous', enableServerCertificateValidation: true }
  }
}

resource lake 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: factory
  name: 'LS_LAKE'
  properties: {
    type: 'AzureBlobFS'
    description: 'ADLS Gen2, reached with the factory\'s managed identity (no key or secret is stored).'
    typeProperties: { url: storageDfsUrl }
  }
}

// ---------- datasets ----------

resource sourceFile 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  parent: factory
  name: 'DS_NHSBSA_MONTHLY_FILE'
  properties: {
    type: 'Binary'
    description: 'One published monthly CSV, copied as bytes so nothing about it is changed on the way in.'
    linkedServiceName: { referenceName: nhsbsa.name, type: 'LinkedServiceReference' }
    parameters: { relativeUrl: { type: 'String' } }
    typeProperties: {
      location: { type: 'HttpServerLocation', relativeUrl: { value: '@dataset().relativeUrl', type: 'Expression' } }
    }
  }
}

resource landedFile 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  parent: factory
  name: 'DS_RAW_MONTHLY_FILE'
  properties: {
    type: 'Binary'
    description: 'The landed copy, gzipped. A 7 GB month lands at roughly a tenth of that.'
    linkedServiceName: { referenceName: lake.name, type: 'LinkedServiceReference' }
    parameters: { fileName: { type: 'String' } }
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        fileSystem: 'raw'
        folderPath: 'epd'
        fileName: { value: '@dataset().fileName', type: 'Expression' }
      }
      compression: { type: 'GZip', level: 'Optimal' }
    }
  }
}

// ---------- pipeline ----------

var landedName = '''@concat('epd_', substring(item().name, 4, 6), '.csv.gz')'''

resource pipeline 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  parent: factory
  name: 'PL_INGEST_EPD'
  properties: {
    description: 'Lands every published month in the window that is not already in the lake, then runs the Databricks job.'
    parameters: {
      windowStart: { type: 'String', defaultValue: '202407' }
      windowEnd: { type: 'String', defaultValue: '999999' }
      forceReload: { type: 'Bool', defaultValue: false }
      databricksHost: { type: 'String', defaultValue: databricksWorkspaceUrl }
      databricksJobId: { type: 'String', defaultValue: databricksJobId }
    }
    variables: {
      runState: { type: 'String', defaultValue: 'PENDING' }
      resultState: { type: 'String', defaultValue: '' }
    }
    activities: [
      {
        name: 'Read catalogue'
        type: 'WebActivity'
        description: 'The publisher\'s CKAN API lists every monthly file with its download URL.'
        policy: { retry: 3, retryIntervalInSeconds: 60, timeout: '0.00:05:00' }
        typeProperties: {
          method: 'GET'
          url: 'https://opendata.nhsbsa.net/api/3/action/package_show?id=english-prescribing-data-epd'
        }
      }
      {
        name: 'Months in window'
        type: 'Filter'
        dependsOn: [ { activity: 'Read catalogue', dependencyConditions: [ 'Succeeded' ] } ]
        typeProperties: {
          items: { value: '@activity(\'Read catalogue\').output.result.resources', type: 'Expression' }
          condition: {
            value: '''@and(startsWith(item().name, 'EPD_'), and(greaterOrEquals(substring(item().name, 4, 6), pipeline().parameters.windowStart), lessOrEquals(substring(item().name, 4, 6), pipeline().parameters.windowEnd)))'''
            type: 'Expression'
          }
        }
      }
      {
        name: 'Each month'
        type: 'ForEach'
        dependsOn: [ { activity: 'Months in window', dependencyConditions: [ 'Succeeded' ] } ]
        typeProperties: {
          items: { value: '@activity(\'Months in window\').output.Value', type: 'Expression' }
          isSequential: false
          batchCount: 4
          activities: [
            {
              name: 'Already landed'
              type: 'GetMetadata'
              typeProperties: {
                dataset: { referenceName: landedFile.name, type: 'DatasetReference', parameters: { fileName: { value: landedName, type: 'Expression' } } }
                fieldList: [ 'exists' ]
                storeSettings: { type: 'AzureBlobFSReadSettings' }
              }
            }
            {
              name: 'Needs landing'
              type: 'IfCondition'
              dependsOn: [ { activity: 'Already landed', dependencyConditions: [ 'Succeeded' ] } ]
              typeProperties: {
                expression: {
                  value: '@or(not(activity(\'Already landed\').output.exists), pipeline().parameters.forceReload)'
                  type: 'Expression'
                }
                ifTrueActivities: [
                  {
                    name: 'Land month'
                    type: 'Copy'
                    policy: { retry: 2, retryIntervalInSeconds: 120, timeout: '0.02:00:00' }
                    inputs: [ {
                      referenceName: sourceFile.name
                      type: 'DatasetReference'
                      parameters: { relativeUrl: { value: '''@replace(item().url, 'https://opendata.nhsbsa.net/', '')''', type: 'Expression' } }
                    } ]
                    outputs: [ {
                      referenceName: landedFile.name
                      type: 'DatasetReference'
                      parameters: { fileName: { value: landedName, type: 'Expression' } }
                    } ]
                    typeProperties: {
                      source: { type: 'BinarySource', storeSettings: { type: 'HttpReadSettings', requestMethod: 'GET' } }
                      sink: { type: 'BinarySink', storeSettings: { type: 'AzureBlobFSWriteSettings' } }
                      dataIntegrationUnits: 4
                    }
                  }
                ]
              }
            }
          ]
        }
      }
      {
        name: 'Start Databricks job'
        type: 'WebActivity'
        dependsOn: [ { activity: 'Each month', dependencyConditions: [ 'Succeeded' ] } ]
        policy: { retry: 2, retryIntervalInSeconds: 60, timeout: '0.00:05:00' }
        typeProperties: {
          method: 'POST'
          url: { value: '''@concat('https://', pipeline().parameters.databricksHost, '/api/2.1/jobs/run-now')''', type: 'Expression' }
          body: { value: '''@json(concat('{"job_id": ', pipeline().parameters.databricksJobId, '}'))''', type: 'Expression' }
          authentication: { type: 'MSI', resource: '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d' }   // Azure Databricks
        }
      }
      {
        name: 'Wait for job'
        type: 'Until'
        dependsOn: [ { activity: 'Start Databricks job', dependencyConditions: [ 'Succeeded' ] } ]
        typeProperties: {
          expression: {
            value: '''@contains(createArray('TERMINATED', 'INTERNAL_ERROR', 'SKIPPED'), variables('runState'))'''
            type: 'Expression'
          }
          timeout: '0.04:00:00'
          activities: [
            { name: 'Pause', type: 'Wait', typeProperties: { waitTimeInSeconds: 60 } }
            {
              name: 'Check job'
              type: 'WebActivity'
              dependsOn: [ { activity: 'Pause', dependencyConditions: [ 'Succeeded' ] } ]
              typeProperties: {
                method: 'GET'
                url: {
                  value: '''@concat('https://', pipeline().parameters.databricksHost, '/api/2.1/jobs/runs/get?run_id=', string(activity('Start Databricks job').output.run_id))'''
                  type: 'Expression'
                }
                authentication: { type: 'MSI', resource: '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d' }
              }
            }
            {
              name: 'Record state'
              type: 'SetVariable'
              dependsOn: [ { activity: 'Check job', dependencyConditions: [ 'Succeeded' ] } ]
              typeProperties: { variableName: 'runState', value: { value: '@activity(\'Check job\').output.state.life_cycle_state', type: 'Expression' } }
            }
            {
              name: 'Record result'
              type: 'SetVariable'
              dependsOn: [ { activity: 'Record state', dependencyConditions: [ 'Succeeded' ] } ]
              typeProperties: {
                variableName: 'resultState'
                value: { value: '''@coalesce(activity('Check job').output.state.result_state, '')''', type: 'Expression' }
              }
            }
          ]
        }
      }
      {
        name: 'Job succeeded'
        type: 'IfCondition'
        dependsOn: [ { activity: 'Wait for job', dependencyConditions: [ 'Succeeded' ] } ]
        typeProperties: {
          expression: { value: '''@equals(variables('resultState'), 'SUCCESS')''', type: 'Expression' }
          ifFalseActivities: [
            {
              name: 'Fail pipeline'
              type: 'Fail'
              typeProperties: {
                message: { value: '''@concat('Databricks job ended ', variables('runState'), ' / ', variables('resultState'))''', type: 'Expression' }
                errorCode: 'DATABRICKS_JOB_FAILED'
              }
            }
          ]
        }
      }
    ]
  }
}

// Monthly schedule. Triggers are created stopped, so nothing runs (or costs) until it is started on purpose.
resource monthly 'Microsoft.DataFactory/factories/triggers@2018-06-01' = {
  parent: factory
  name: 'TR_MONTHLY'
  properties: {
    type: 'ScheduleTrigger'
    description: 'On the 20th of each month, pick up any newly published month.'
    pipelines: [ { pipelineReference: { referenceName: pipeline.name, type: 'PipelineReference' } } ]
    typeProperties: {
      recurrence: {
        frequency: 'Month'
        interval: 1
        startTime: scheduleStart
        timeZone: 'UTC'
        schedule: { monthDays: [ 20 ], hours: [ 6 ], minutes: [ 0 ] }
      }
    }
  }
}

output name string = factory.name
output principalId string = factory.identity.principalId
