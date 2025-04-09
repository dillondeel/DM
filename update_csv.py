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
    "Category": "Categories"  # Direct linked field in Fundraising Rounds
}

# Additional mappings for lookups in Companies table (not needed since we're removing these columns)
lookup_mappings = {
    "Category (from Company)": "Categories",
    "Sector (from Company)": "Sectors"
}

# Fetch data from linked tables
linked_data = {}
for field, table in linked_tables.items():
    url = f"https://api.airtable.com/v0/{base_id}/{table}"
    all_linked_records = []
    offset = None
    while True:
        params = {"offset": offset} if offset else {}
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        all_linked_records.extend(data["records"])
        offset = data.get("offset")
        if not offset:
            break
    if field == "Investors":
        linked_data[field] = {record["id"]: record["fields"].get("Fund", record["id"]) for record in all_linked_records}
    elif field == "Sector":
        linked_data[field] = {record["id"]: record["fields"].get("Sector", record["id"]) for record in all_linked_records}
    elif field == "Category":
        linked_data[field] = {record["id"]: record["fields"].get("Category", record["id"]) for record in all_linked_records}
    elif field == "Company":
        linked_data[field] = {record["id"]: record["fields"].get("Company", record["id"]) for record in all_linked_records}
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
    # Transform direct linked fields
    for field in linked_tables.keys():
        if field in transformed:
            value = transformed[field]
            if isinstance(value, list):
                mapped_values = [linked_data[field].get(id, id) for id in value if id in linked_data[field]]
                transformed[field] = ", ".join(mapped_values) if mapped_values else ""
            else:
                transformed[field] = linked_data[field].get(value, value)
    transformed_records.append(transformed)

# Filter out unwanted columns
fields = list(fields)
unwanted_columns = ["Source", "Sector (from Company)", "Category (from Company)"]
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
