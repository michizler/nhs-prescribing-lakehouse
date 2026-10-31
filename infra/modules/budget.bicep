// Monthly cost budget for the resource group, with email alerts as spend passes 50%, 80% and 100%.
param amount int
param contactEmail string
param startDate string

resource budget 'Microsoft.Consumption/budgets@2023-11-01' = {
  name: 'epd-lakehouse-monthly'
  properties: {
    category: 'Cost'
    amount: amount
    timeGrain: 'Monthly'
    timePeriod: { startDate: startDate }
    notifications: {
      actual50: { enabled: true, operator: 'GreaterThan', threshold: 50, thresholdType: 'Actual', contactEmails: [ contactEmail ] }
      actual80: { enabled: true, operator: 'GreaterThan', threshold: 80, thresholdType: 'Actual', contactEmails: [ contactEmail ] }
      forecast100: { enabled: true, operator: 'GreaterThan', threshold: 100, thresholdType: 'Forecasted', contactEmails: [ contactEmail ] }
    }
  }
}
