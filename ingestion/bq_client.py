"""
bq_client.py
------------
Thin wrapper around the BigQuery Python client.
Handles auth and gives us a single client instance to reuse.
"""

import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()


def get_client() -> bigquery.Client:
    """Return an authenticated BigQuery client."""
    project = os.environ["GCP_PROJECT_ID"]
    # GOOGLE_APPLICATION_CREDENTIALS is picked up automatically
    # by the google-cloud library if set in the environment.
    return bigquery.Client(project=project)


def run_query(sql: str, client: bigquery.Client | None = None) -> list[dict]:
    """Run a SQL query and return results as a list of dicts."""
    client = client or get_client()
    job = client.query(sql)
    return [dict(row) for row in job.result()]
