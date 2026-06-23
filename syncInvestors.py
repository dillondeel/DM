import requests
import csv
import io
import os
import sys

# Airtable setup
base_id = "appQY8Pco55RA0JSp"
main_table = "Investors"

api_key = os.getenv("AIRTABLE_API_KEY")
if not api_key:
    print("Error: AIRTABLE_API_KEY environment variable is not set.")
    sys.exit(1)

headers = {"Authorization": f"Bearer {api_key}"}

# Linked tables (fields that return IDs)
linked_tables = {
    "Funded Rounds": "Fundraising%20Rounds%20-%20Companies",
    "Companies Funded": "Companies"  # Added to handle Companies Funded field
}


def fetch_all_records(url, headers):
    """Fetch all paginated records from an Airtable endpoint."""
    all_records = []
    offset = None
    while True:
        params = {"offset": offset} if offset else {}
        try:
            response = requests.get(url, headers=headers, params=params)
        except requests.RequestException as exc:
            raise RuntimeError(f"Network error while fetching {url}: {exc}") from exc

        if response.status_code != 200:
            raise RuntimeError(
                f"Airtable API error {response.status_code} for {url}: {response.text}"
            )

        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Invalid JSON response from {url}: {response.text[:200]}"
            ) from exc

        if "records" not in data:
            raise RuntimeError(
                f"Unexpected response structure from {url} (missing 'records' key): "
                f"{response.text[:200]}"
            )

        all_records.extend(data["records"])
        offset = data.get("offset")
        if not offset:
            break
    return all_records


# Fetch data from linked tables
linked_data = {}
for field, table in linked_tables.items():
    url = f"https://api.airtable.com/v0/{base_id}/{table}"
    try:
        all_linked_records = fetch_all_records(url, headers)
    except RuntimeError as exc:
        print(f"Error fetching linked table '{table}' for field '{field}': {exc}")
        sys.exit(1)

    if field == "Funded Rounds":
        linked_data[field] = {record["id"]: record["fields"].get("Fundraising Round", record["id"]) for record in all_linked_records}
        print(f"Sample mapping for {field}: {list(linked_data[field].items())[:3]}")  # Debug: Show some mappings
    elif field == "Companies Funded":
        linked_data[field] = {record["id"]: record["fields"].get("Company", record["id"]) for record in all_linked_records}
        print(f"Sample mapping for {field}: {list(linked_data[field].items())[:3]}")  # Debug: Show some mappings
    if all_linked_records:
        print(f"Sample {table} record: {all_linked_records[0]['fields']}")
        print(f"Total {table} records fetched: {len(all_linked_records)}")

# Fetch all records from the "Investors" table with pagination
url_main = f"https://api.airtable.com/v0/{base_id}/{main_table}"
try:
    all_records = fetch_all_records(url_main, headers)
except RuntimeError as exc:
    print(f"Error fetching main table '{main_table}': {exc}")
    sys.exit(1)

# Debug: Print sample raw record from the "Investors" table
if all_records:
    print(f"Sample Investors record: {all_records[0]['fields']}")

# Extract fields and transform linked records
fields = set()
transformed_records = []
unwanted_columns = []
for record in all_records:
    fields.update(record["fields"].keys())
    transformed = record["fields"].copy()
    # Transform direct linked fields
    for field in linked_tables.keys():
        if field in transformed:
            value = transformed[field]
            if isinstance(value, list):
                mapped_values = [linked_data[field].get(id, id) for id in value]
                transformed[field] = ", ".join(mapped_values) if mapped_values else ""
                print(f"Transformed {field} for {transformed.get('Fund', 'unknown')}: {transformed[field]}")  # Debug: Trace transformation
            else:
                transformed[field] = linked_data[field].get(value, value) if value else ""
    # Handle Unique Companies as a formula field
    if "Unique Companies" in transformed:
        value = transformed["Unique Companies"]
        if isinstance(value, list):
            transformed["Unique Companies"] = ", ".join(value)
        elif isinstance(value, str):
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
output_path = "data/Investors.csv"
output_dir = os.path.dirname(output_path)
if output_dir:
    os.makedirs(output_dir, exist_ok=True)

try:
    with open(output_path, "w") as f:
        f.write(output.getvalue())
except OSError as exc:
    print(f"Error writing output file '{output_path}': {exc}")
    sys.exit(1)

print(f"Successfully wrote {len(transformed_records)} records to {output_path}")
