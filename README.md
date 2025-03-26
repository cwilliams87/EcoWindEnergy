# EcoWind Energy Data Pipeline

This project implements a data processing pipeline for wind turbine data using Databricks Asset Bundles. The pipeline follows the medallion architecture pattern and processes data from wind turbines to generate insights and identify anomalies.

## Project Structure

```
ecowind_energy/
├── src/
│   ├── bronze/          # Bronze layer ingestion logic
│   ├── silver/          # Silver layer transformation logic
│   └── utils/           # Utility functions and configurations
├── notebooks/
│   ├── bronze/          # Bronze layer notebooks
│   └── silver/          # Silver layer notebooks
├── tests/               # Unit tests
├── resources/           # Configuration files
└── databricks.yml       # Databricks Asset Bundle configuration
```

## Features

- **Bronze Layer:**
  - Ingests raw CSV data from ADLS using Autoloader
  - Implements data quality checks
  - Quarantines invalid records
  - Stores data in Delta Lake format

- **Silver Layer:**
  - Calculates summary statistics for each turbine
  - Identifies anomalies using z-score analysis
  - Stores processed data in Delta format

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- ADLS Gen2 storage account with service principal access
- Databricks CLI installed and configured

## Configuration

1. Create a secret scope named "EcoWindCreds" with the following secrets:
   - `client_id`: Service principal client ID
   - `client_secret`: Service principal client secret
   - `tenant_id`: Azure AD tenant ID

2. Update the `resources/config.yaml` file with your storage account details

## Deployment

1. Build the asset bundle:
   ```bash
   databricks bundle deploy
   ```

2. The pipeline will be deployed with two jobs:
   - Bronze Ingestion: Ingests raw data from ADLS
   - Silver Transformation: Processes data and calculates statistics

## Data Quality Rules

- Power output must be between 0 and 100 MW
- Wind speed must be between 0 and 100 m/s
- Maximum allowed missing values: 5%
- Anomalies are identified as values outside 2 standard deviations

## Testing

Run unit tests using pytest:
```bash
pytest tests/
```

## Monitoring

The pipeline includes:
- Data quality metrics tracking
- Quarantine table for invalid records
- Anomaly detection and flagging 