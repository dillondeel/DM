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
    "Category": "Categories"
}


def fetch_linked_table(url, headers):
    """Fetch all records from a linked table with pagination."""
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
    return all_linked_records


def build_linked_data(linked_tables, base_id, headers):
    """Fetch data from all linked tables and build lookup dictionaries."""
    linked_data = {}
    for field, table in linked_tables.items():
        url = f"https://api.airtable.com/v0/{base_id}/{table}"
        all_linked_records = fetch_linked_table(url, headers)
        if field == "Investors":
            linked_data[field] = {
                record["id"]: record["fields"].get("Fund", record["id"])
                for record in all_linked_records
            }
        elif field == "Sector":
            linked_data[field] = {
                record["id"]: record["fields"].get("Sector", record["id"])
                for record in all_linked_records
            }
        elif field == "Category":
            linked_data[field] = {
                record["id"]: record["fields"].get("Category", record["id"])
                for record in all_linked_records
            }
        elif field == "Company":
            linked_data[field] = {
                record["id"]: record["fields"].get("Company", record["id"])
                for record in all_linked_records
            }
        else:
            linked_data[field] = {
                record["id"]: record["fields"].get("Name", record["id"])
                for record in all_linked_records
            }
        if all_linked_records:
            print(f"Sample {table} record: {all_linked_records[0]['fields']}")
            print(f"Total {table} records fetched: {len(all_linked_records)}")
    return linked_data


def fetch_main_table(url, headers):
    """Fetch all records from the main table with pagination."""
    all_records = []
    offset = None
    while True:
        params = {"offset": offset} if offset else {}
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        all_records.extend(data["records"])
        offset = data.get("offset")
        if not offset:
            break
    return all_records


def transform_records(all_records, linked_tables, linked_data):
    """Transform records by resolving linked IDs and removing unwanted columns."""
    fields = set()
    transformed_records = []
    unwanted_columns = ["Source", "Sector (from Company)", "Category (from Company)"]
    for record in all_records:
        fields.update(record["fields"].keys())
        transformed = record["fields"].copy()
        for field in linked_tables.keys():
            if field in transformed:
                value = transformed[field]
                if isinstance(value, list):
                    mapped_values = [
                        linked_data[field].get(id, id)
                        for id in value
                        if id in linked_data[field]
                    ]
                    transformed[field] = ", ".join(mapped_values) if mapped_values else ""
                else:
                    transformed[field] = linked_data[field].get(value, value)
        for col in unwanted_columns:
            transformed.pop(col, None)
        transformed_records.append(transformed)
    fields = [field for field in fields if field not in unwanted_columns]
    return fields, transformed_records


def write_csv(fields, transformed_records, output_path):
    """Convert transformed records to CSV and write to file."""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for record in transformed_records:
        writer.writerow(record)
    with open(output_path, "w") as f:
        f.write(output.getvalue())


def main():
    linked_data = build_linked_data(linked_tables, base_id, headers)
    url_main = f"https://api.airtable.com/v0/{base_id}/{main_table}"
    all_records = fetch_main_table(url_main, headers)
    if all_records:
        print(f"Sample Fundraising Rounds record: {all_records[0]['fields']}")
    fields, transformed_records = transform_records(all_records, linked_tables, linked_data)
    write_csv(fields, transformed_records, "data/fundraising_rounds_companies.csv")


if __name__ == "__main__":
    main()
