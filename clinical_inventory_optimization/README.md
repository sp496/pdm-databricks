# Clinical Inventory Optimization

## Overview

<!-- Describe the project purpose here -->

## Project Structure

```
clinical_inventory_optimization/
├── config/          # Environment and layer configuration (JSON)
├── ddl/             # Schema setup — run once before the pipeline
├── lib/             # Importable Python modules (business logic)
│   ├── raw/
│   ├── curated/
│   ├── processed/
│   └── metadata/
├── notebooks/       # Databricks notebooks — orchestrate the pipeline
│   ├── raw/
│   ├── curated/
│   ├── processed/
│   └── metadata/
├── tests/
│   ├── unit/        # Unit tests for lib/ modules
│   └── integration/ # End-to-end pipeline tests
└── requirements.txt
```

## Pipeline Layers

| Layer | Purpose |
|-------|---------|
| `raw` | Ingest and decrypt source files from S3 |
| `curated` | Standardise and map to target schema |
| `processed` | Demand planning and forecasting |
| `metadata` | Pipeline observability — load status and data quality |

## Setup

<!-- Add environment setup instructions here -->

## Running Tests

```bash
pip install -r requirements.txt
pytest tests/unit/
```
