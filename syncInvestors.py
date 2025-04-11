import requests
import csv
import io
import os

# Airtable setup
base_id = "appQY8Pco55RA0JSp"
main_table = "Investors"
headers = {"Authorization": f"Bearer {os.getenv('AIRTABLE_API_KEY')}"}

# Linked tables (fields that return IDs)
linked_tables = {
    "Funded Rounds": "Fundraising%20Rounds%20-%20Companies"
    # Removed "Unique Companies" since it's a formula field, not a linked field
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
    if field == "Funded Rounds":
        # Use the correct field name "Fundraising Round"
        linked_data[field] = {}
        for record in all_linked_records:
            fields = record["fields"]
            name = fields.get("Fundraising Round", record["id"])  # Fallback to record ID if not found
            linked_data[field][record["id"]] = name
        print(f"Sample mapping for {field}: {list(linked_data[field].items())[:3]}")  # Debug: Show some mappings
    if all_linked_records:
        print(f"Sample {table} record: {all_linked_records[0]['fields']}")
        print(f"Total {table} records fetched: {len(all_linked_records)}")

# Fetch all records from the "Investors" table with pagination
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

# Debug: Print sample raw record from the "Investors" table
if all_records:
    print(f"Sample Investors record: {all_records[0]['fields']}")

# Extract fields and transform linked records
fields = set()
transformed_records = []
unwanted_columns = []  # No unwanted columns specified for Investors
for record in all_records:
    fields.update(record["fields"].keys())
    transformed = record["fields"].copy()
    # Transform direct linked fields
    for field in linked_tables.keys():
        if field in transformed:
            value = transformed[field]
            if isinstance(value, list):
                # Map IDs to names
                mapped_values = []
                for id in value:
                    if id in linked_data[field]:
                        mapped_values.append(linked_data[field][id])
                    else:
                        print(f"Warning: ID {id} not found in {field} linked data")
                        mapped_values.append(id)  # Fallback to ID if not found
                transformed[field] = ", ".join(mapped_values) if mapped_values else ""
            else:
                transformed[field] = linked_data[field].get(value, value) if value else ""
    # Handle Unique Companies as a formula field
    if "Unique Companies" in transformed:
        value = transformed["Unique Companies"]
        if isinstance(value, list):
            # If it's a list (e.g., from a formula field), join it into a string
            transformed["Unique Companies"] = ", ".join(value)
        elif isinstance(value, str):
            # If it's already a string, use it as-is
            transformed["Unique Companies"] = value
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
with open("data/Investors.csv", "w") as f:
    f.write(output.getvalue())
