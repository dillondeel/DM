import requests
import csv
import io
import os

# Airtable setup
base_id = "appQY8Pco55RA0JSp"
main_table = "Fundraising%20Rounds%20-%20Companies"
headers = {"Authorization": f"Bearer {os.getenv('AIRTABLE_API_KEY')}"}

# Linked tables (fields that return IDs)
linked_tables = {
    "company": "Companies",    # Links to "Companies" table
    "investors": "Investors",  # Links to "Investors" table
    "sector": "Sectors"        # Links to "Sectors" table
}

# Fetch data from linked tables
linked_data = {}
for field, table in linked_tables.items():
    url = f"https://api.airtable.com/v0/{base_id}/{table}"
    all_linked_records = []
    offset = None
    while True:  # Pagination for linked tables
        params = {"offset": offset} if offset else {}
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        all_linked_records.extend(data["records"])
        offset = data.get("offset")
        if not offset:
            break
    # Map record IDs to display field ("Fund" for investors, "Name" for others)
    if field == "investors":
        linked_data[field] = {record["id"]: record["fields"].get("Fund", record["id"]) for record in all_linked_records}
        if all_linked_records:
            print(f"Sample Investors record: {all_linked_records[0]['fields']}")
            print(f"Total Investors records fetched: {len(all_linked_records)}")
    else:
        linked_data[field] = {record["id"]: record["fields"].get("Name", record["id"]) for record in all_linked_records}

# Fetch all records from main table with pagination
url_main = f"https://api.airtable.com/v0/{base_id}/{main_table}"
all_records = []
offset = None
while True:
    params = {"offset": offset} if offset else {}
    response = requests.get(url_main, headers=headers, params=params)
    data = response.json()
    all_records.extend(data["records"])
    offset = data.get("offset")
    if not offset:
        break

# Debug: Print sample raw record from main table
if all_records:
    print(f"Sample Fundraising Rounds record: {all_records[0]['fields']}")

# Extrac
