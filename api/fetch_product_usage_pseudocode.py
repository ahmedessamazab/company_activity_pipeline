"""
Pseudocode for product-usage API ingestion.

This function loops through a date range, calls the product usage API,
normalizes the response, and uploads each day's JSON to Azure Blob Storage.

This is NOT the final production script — it is intentionally simplified
to demonstrate the ingestion logic for the assessment task.
"""

import os
from dotenv import load_dotenv
from datetime import timedelta

# Load .env file if present
load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
AZURE_CONN = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

if not API_TOKEN:
    raise EnvironmentError("Missing environment variable: API_TOKEN")

if not AZURE_CONN:
    raise EnvironmentError(
        "Missing environment variable: AZURE_STORAGE_CONNECTION_STRING"
    )


def fetch_product_usage(start_date, end_date, blob_path):
    """
    Calls the product usage API for a date range
    and writes each day's response to Azure Blob Storage.
    """

    import requests
    from azure.storage.blob import BlobClient
    import json

    current = start_date

    while current <= end_date:

        # Convert date object to ISO string
        date_str = current.isoformat()

        # Build daily endpoint
        url = f"https://api.company.com/product-usage?date={date_str}"

        # API call
        response = requests.get(url, headers={"Authorization": f"Bearer {API_TOKEN}"})
        data = response.json()

        # Normalize rows
        rows = []
        for record in data.get("usage", []):
            rows.append(
                {
                    "company_id": record.get("company_id"),
                    "date": record.get("date", date_str),
                    "active_users": record.get("active_users", 0),
                    "events": record.get("events", 0),
                }
            )

        # Upload to Blob as JSON
        blob_name = f"{blob_path}/usage_{date_str}.json"
        blob = BlobClient.from_connection_string(
            AZURE_CONN, container="staging", blob_name=blob_name
        )

        blob.upload_blob(json.dumps(rows), overwrite=True)

        # Next day
        current += timedelta(days=1)
