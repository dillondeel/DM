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
    "Company": "Companies",
    "Investors": "Investors",
    "Sector": "Sectors",
    "Category (from Company)": "Categories",  # Treat as linked field
    "Sector (from Company)": "Sectors"       # Treat as linked field
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
    # Map record IDs to display field ("Fund" for Investors, "Name" for others)
    if field == "Investors":
        linked_data[field] = {record["id"]: record["fields"].get("Fund", record["id"]) for record in all_linked_records}
    else:
        linked_data[field] = {record["id"]: record["fields"].get("Name", record["id"]) for record in all_linked_records}
    if all_linked_records:
        print(f"Sample {table} record: {all_linked_records[0]['fields']}")
        print(f"Total {table} records fetched: {len(all_linked_records)}")

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

# Extract fields and transform linked records
fields = set()
transformed_records = []
for record in all_records:
    fields.update(record["fields"].keys())
    transformed = record["fields"].copy()
    for field in linked_tables.keys():
        if field in transformed:
            value = transformed[field]
            if isinstance(value, list):  # Handle multiple linked records
                mapped_values = [linked_data[field].get(id, id) for id in value if id in linked_data[field]]
                transformed[field] = ", ".join(mapped_values) if mapped_values else ""
            else:  # Handle single linked record
                transformed[field] = linked_data[field].get(value, value)
    transformed_records.append(transformed)
fields = list(fields)

# Convert to CSV
output = io.StringIO()
writer = csv.DictWriter(output, fieldnames=fields)
writer.writeheader()
for record in transformed_records:
    writer.writerow(record)

# Write to file
with open("data/fundraising_rounds_companies.csv", "w") as f:
    f.write(output.getvalue())
