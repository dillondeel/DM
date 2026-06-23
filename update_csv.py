from airtable_utils import (
    build_linked_data,
    fetch_all_records,
    get_headers,
    remove_unwanted_columns,
    resolve_linked_fields,
    write_records_to_csv,
)

# Airtable setup
headers = get_headers()
main_table = "Fundraising%20Rounds%20-%20Companies"

# Linked tables (fields that return IDs) and their display fields
linked_tables = {
    "Company": "Companies",
    "Investors": "Investors",
    "Sector": "Sectors",
    "Category": "Categories",
}
field_mappings = {
    "Company": "Company",
    "Investors": "Fund",
    "Sector": "Sector",
    "Category": "Category",
}

# Fetch and build linked data mappings
linked_data = build_linked_data(linked_tables, field_mappings, headers=headers)

# Fetch all records from main table
all_records = fetch_all_records(main_table, headers=headers)

if all_records:
    print(f"Sample Fundraising Rounds record: {all_records[0]['fields']}")

# Extract fields and transform linked records
fields = set()
transformed_records = []
unwanted_columns = ["Source", "Sector (from Company)", "Category (from Company)"]

for record in all_records:
    fields.update(record["fields"].keys())
    transformed = record["fields"].copy()

    resolve_linked_fields(transformed, linked_tables, linked_data)
    remove_unwanted_columns(transformed, unwanted_columns)
    transformed_records.append(transformed)

# Filter out unwanted columns from fields
fields = [field for field in fields if field not in unwanted_columns]

# Write to CSV
write_records_to_csv(transformed_records, fields, "data/fundraising_rounds_companies.csv")
