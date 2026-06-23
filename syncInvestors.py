from airtable_utils import (
    build_linked_data,
    fetch_all_records,
    get_headers,
    remove_unwanted_columns,
    write_records_to_csv,
)

# Airtable setup
headers = get_headers()
main_table = "Investors"

# Linked tables (fields that return IDs) and their display fields
linked_tables = {
    "Funded Rounds": "Fundraising%20Rounds%20-%20Companies",
    "Companies Funded": "Companies",
}
field_mappings = {
    "Funded Rounds": "Fundraising Round",
    "Companies Funded": "Company",
}

# Fetch and build linked data mappings
linked_data = build_linked_data(linked_tables, field_mappings, headers=headers)

# Print sample mappings for debugging
for field in linked_tables:
    print(f"Sample mapping for {field}: {list(linked_data[field].items())[:3]}")

# Fetch all records from the main table
all_records = fetch_all_records(main_table, headers=headers)

if all_records:
    print(f"Sample Investors record: {all_records[0]['fields']}")

# Extract fields and transform linked records
fields = set()
transformed_records = []
unwanted_columns = []

for record in all_records:
    fields.update(record["fields"].keys())
    transformed = record["fields"].copy()

    # Resolve linked field IDs to display values (include all IDs, even unmatched)
    for field in linked_tables:
        if field in transformed:
            value = transformed[field]
            if isinstance(value, list):
                mapped_values = [linked_data[field].get(id, id) for id in value]
                transformed[field] = ", ".join(mapped_values) if mapped_values else ""
                print(f"Transformed {field} for {transformed.get('Fund', 'unknown')}: {transformed[field]}")
            else:
                transformed[field] = linked_data[field].get(value, value) if value else ""

    # Handle Unique Companies as a formula field
    if "Unique Companies" in transformed:
        value = transformed["Unique Companies"]
        if isinstance(value, list):
            transformed["Unique Companies"] = ", ".join(value)
        elif isinstance(value, str):
            transformed["Unique Companies"] = value

    remove_unwanted_columns(transformed, unwanted_columns)
    transformed_records.append(transformed)

# Filter out unwanted columns from fields
fields = [field for field in fields if field not in unwanted_columns]

# Write to CSV
write_records_to_csv(transformed_records, fields, "data/Investors.csv")
