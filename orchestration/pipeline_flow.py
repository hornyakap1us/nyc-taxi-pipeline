"""
pipeline_flow.py
----------------
Prefect flow that orchestrates the full pipeline:
  1. Ingest yesterday's taxi data into BigQuery raw layer
  2. Run dbt models (staging → intermediate → marts)
  3. Run dbt tests to validate output
  4. (Optional) notify on failure

Run locally:    python pipeline_flow.py
Deploy to cloud: prefect deploy pipeline_flow.py:nyc_taxi_pipeline

Free Prefect Cloud setup:
  1. Sign up at https://app.prefect.io
  2. `prefect cloud login` and paste your API key
  3. `prefect deploy` to register this flow
  4. Set a schedule in the Prefect UI (e.g. daily at 6am)
"""

import subprocess
import os
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect.client.schemas.schedules import CronSchedule

load_dotenv()

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
INGESTION_DIR = PROJECT_ROOT / "ingestion"
DBT_DIR       = PROJECT_ROOT / "dbt_project"


# ─────────────────────────────────────────────
# Tasks
# ─────────────────────────────────────────────

@task(name="ingest-raw-data", retries=2, retry_delay_seconds=60)
def ingest_raw_data(year: int, month: int) -> int:
    logger = get_run_logger()
    logger.info(f"Ingesting data for {year}-{month:02d}")

    import sys
    sys.path.insert(0, str(INGESTION_DIR))
    from ingest import ingest_month
    from bq_client import get_client

    client = get_client()
    rows = ingest_month(year, month, client)
    logger.info(f"Ingested {rows:,} rows")
    return rows


@task(name="dbt-run", retries=1, retry_delay_seconds=30)
def dbt_run() -> None:
    """Execute dbt run — builds all models."""
    logger = get_run_logger()
    logger.info("Running dbt models...")

    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", str(DBT_DIR)],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
    )

    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")

    logger.info("dbt run completed successfully.")


@task(name="dbt-test", retries=0)
def dbt_test() -> None:
    """Execute dbt test — validates all models."""
    logger = get_run_logger()
    logger.info("Running dbt tests...")

    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", str(DBT_DIR)],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
    )

    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt test failed:\n{result.stderr}")

    logger.info("All dbt tests passed.")


@task(name="dbt-docs", retries=0)
def dbt_generate_docs() -> None:
    """Regenerate dbt docs (lineage graph)."""
    logger = get_run_logger()
    logger.info("Generating dbt docs...")

    subprocess.run(
        ["dbt", "docs", "generate", "--profiles-dir", str(DBT_DIR)],
        cwd=DBT_DIR,
        check=True,
    )
    logger.info("Docs generated. Run `dbt docs serve` to view lineage.")


# ─────────────────────────────────────────────
# Flow
# ─────────────────────────────────────────────

@flow(
    name="nyc-taxi-pipeline",
    description="Daily NYC taxi data pipeline: ingest → dbt run → dbt test",
    # Uncomment to add a schedule when deployed to Prefect Cloud:
    # schedule=CronSchedule(cron="0 6 * * *", timezone="America/New_York"),
)
def nyc_taxi_pipeline(target_date: date | None = None) -> None:
    """
    Full pipeline flow.

    Args:
        target_date: Date to ingest. Defaults to yesterday.
    """
    logger = get_run_logger()

    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    logger.info(f"Starting pipeline for {target_date}")

    # Step 1: Ingest
    rows = ingest_raw_data(2022, 4)

    # Step 2: Transform (dbt run depends on ingest finishing)
    dbt_run(wait_for=[rows])

    # Step 3: Validate
    dbt_test()

    # Step 4: Refresh docs
    dbt_generate_docs()

    logger.info("Pipeline complete.")


# ─────────────────────────────────────────────
# Local entrypoint
# ─────────────────────────────────────────────

if __name__ == "__main__":
    # Run the flow locally for a specific date to test it
    nyc_taxi_pipeline(target_date=date(2023, 6, 1))
