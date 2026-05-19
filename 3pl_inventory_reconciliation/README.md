# 3PL Inventory Reconciliation

Databricks project for reconciling third-party logistics (3PL) inventory data.

## Structure

```
3pl_inventory_reconciliation/
├── config/          # Environment and layer-specific configuration
├── ddl/             # Delta table DDL definitions
├── lib/             # Reusable Python modules
│   ├── raw/
│   ├── curated/
│   ├── metadata/
│   └── processed/
├── notebooks/       # Databricks notebooks
│   ├── raw/
│   ├── curated/
│   ├── metadata/
│   └── processed/
└── tests/           # Unit and integration tests
    ├── fixtures/
    ├── integration/
    └── unit/
```
