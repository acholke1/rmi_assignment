Dataeaze Systems| Assesment round: GCP Engineer+API

# RMI - Data Engineering Assignment

This repository contains a sample implementation of the RMI data engineering assignment:
- Streaming ingestion pipeline (Pub/Sub → Dataflow (Beam) → BigQuery).
- Batch ELT SQL for enrichment using `min_wage`.
- Batch ML prediction pipeline (BigQuery → Dataflow → BigQuery) with a mock model.
- Cloud Build CI step to build, push and launch the Dataflow job.
- GCP Workflow example and cron for scheduler.

> **Note:** These files are sample implementations and include placeholders (PROJECT_ID, dataset, table names, GCS paths). Replace those with your actual project resources before deploying.

## Quick local test
1. Create virtualenv:
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

2. Run ingestion pipeline locally (DirectRunner):
```bash
python -m dataflow.df_ingestion_pipeline --runner=DirectRunner --input_file=sample_data/raw_job_postings.json
```

3. To deploy you will need to:
- Fill GCP project-specific variables in YAMLs and pipeline args.
- Push images and artifacts to Artifact Registry / GCS as appropriate.
- Grant the service account Dataflow/BigQuery/Storage permissions.

See each file for details.
