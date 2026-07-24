import logging
import sys

import requests
import csv
import io
import os

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Airtable setup
base_id = os.getenv("AIRTABLE_BASE_ID", "appQY8Pco55RA0JSp")
main_table = "Fundraising%20Rounds%20-%20Companies"

api_key = os.getenv("AIRTABLE_API_KEY")
if not api_key:
    logger.error("AIRTABLE_API_KEY environment variable is not set")
    sys.exit(1)
headers = {"Authorization": f"Bearer {api_key}"}

# Linked tables (fields that return IDs)
linked_tables = {
    "Company": "Companies",
    "Investors": "Investors",
    "Sector": "Sectors",
    "Category": "Categories"
}

FIELD_NAME_MAP = {
    "Investors": "Fund",
    "Sector": "Sector",
    "Category": "Category",
    "Company": "Company",
}


def fetch_paginated(url, headers):
    """Fetch all records from an Airtable endpoint with pagination."""
    all_records = []
    offset = None
    while True:
        params = {"offset": offset} if offset else {}
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        all_records.extend(data["records"])
        offset = data.get("offset")
        if not offset:
            break
    return all_records


# Fetch data from linked tables
linked_data = {}
for field, table in linked_tables.items():
    url = f"https://api.airtable.com/v0/{base_id}/{table}"
    all_linked_records = fetch_paginated(url, headers)
    name_key = FIELD_NAME_MAP.get(field, "Name")
    linked_data[field] = {
        record["id"]: record["fields"].get(name_key, record["id"])
        for record in all_linked_records
    }
    logger.info("Fetched %d records from %s", len(all_linked_records), table)

# Fetch all records from main table with pagination
url_main = f"https://api.airtable.com/v0/{base_id}/{main_table}"
all_records = fetch_paginated(url_main, headers)
logger.info("Fetched %d Fundraising Rounds records", len(all_records))

# Extract fields and transform linked records
fields = set()
transformed_records = []
unwanted_columns = ["Source", "Sector (from Company)", "Category (from Company)"]
for record in all_records:
    fields.update(record["fields"].keys())
    transformed = record["fields"].copy()
    # Transform direct linked fields
    for field in linked_tables.keys():
        if field in transformed:
            value = transformed[field]
            if isinstance(value, list):
                mapped_values = [linked_data[field].get(id, id) for id in value if id in linked_data[field]]
                transformed[field] = ", ".join(mapped_values) if mapped_values else ""
            else:
                transformed[field] = linked_data[field].get(value, value)
    # Remove unwanted columns from the transformed record
    for col in unwanted_columns:
        transformed.pop(col, None)
    transformed_records.append(transformed)

# Filter out unwanted columns from fields (for safety)
fields = [field for field in fields if field not in unwanted_columns]

# Convert to CSV
output = io.StringIO()
writer = csv.DictWriter(output, fieldnames=fields)
writer.writeheader()
for record in transformed_records:
    writer.writerow(record)

# Write to file
with open("data/fundraising_rounds_companies.csv", "w") as f:
    f.write(output.getvalue())
