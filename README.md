# Automated ETL Pipeline for Network Intelligence

> Secure data pipelines ingesting CSV, SAS7BDAT, and API sources into Delta Lake for real-time network monitoring.

## Role
**Data Engineer** — Designed pipeline architecture and incremental loading strategy.

## Overview
Secure data pipelines to ingest CSV, SAS7BDAT, and API sources (firewall and network device logs) into centralized Delta tables, enabling real-time network health monitoring with anomaly detection.

## Architecture
```
CSV / SAS7BDAT Files → File Watcher → Extractor
Firewall API →                         ↓
SNMP Polling →                  Transformer (Cleaning + Enrichment)
                                       ↓
                           Quality Checks → Delta Lake (Bronze → Silver → Gold)
                                                                ↓
                                              Anomaly Detection → Dashboard
```

## Key Features
- **Multi-Format Ingestion** — CSV, SAS7BDAT, REST API, SNMP
- **Medallion Architecture** — Bronze (raw) → Silver (cleaned) → Gold (aggregated)
- **Data Quality Gates** — Automated checks between layers (null %, schema drift)
- **Incremental Loading** — Merge-based upserts with change detection
- **Network Anomaly Detection** — Statistical baseline alerting on metrics

## Tech Stack
`Databricks` · `Delta Lake` · `SQL` · `Python` · `ETL` · `Network Analytics`

## Impact
- Processing **4.5M+ records/day** with **98% pipeline success rate**
- Reduced execution time from **45 to 28 minutes** via incremental optimization

## Project Structure
```
src/
├── extractor.py        # Multi-format data extraction
├── transformer.py      # Data cleaning & enrichment
├── loader.py           # Delta Lake writer with merge logic
├── quality_checks.py   # Automated data quality validation
└── pipeline.py         # Orchestration & scheduling
config/
└── pipeline_config.yaml
```

## License
MIT
