"""Shared utilities for Airtable data sync scripts."""

import csv
import io
import os

import requests

BASE_ID = "appQY8Pco55RA0JSp"
API_BASE_URL = f"https://api.airtable.com/v0/{BASE_ID}"


def get_headers():
    """Return authorization headers for the Airtable API."""
    return {"Authorization": f"Bearer {os.getenv('AIRTABLE_API_KEY')}"}


def fetch_all_records(table_name, headers=None):
    """Fetch all records from an Airtable table, handling pagination.

    Args:
        table_name: The URL-encoded table name.
        headers: Optional auth headers. Uses get_headers() if not provided.

    Returns:
        A list of all record dicts from the table.
    """
    if headers is None:
        headers = get_headers()

    url = f"{API_BASE_URL}/{table_name}"
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


def build_linked_data(linked_tables, field_mappings, headers=None):
    """Fetch linked tables and build ID-to-display-value mappings.

    Args:
        linked_tables: Dict mapping field names to table names,
            e.g. {"Company": "Companies"}.
        field_mappings: Dict mapping field names to the display field to extract,
            e.g. {"Company": "Company", "Investors": "Fund"}.
        headers: Optional auth headers.

    Returns:
        A dict mapping field names to {record_id: display_value} dicts.
    """
    if headers is None:
        headers = get_headers()

    linked_data = {}
    for field, table in linked_tables.items():
        records = fetch_all_records(table, headers=headers)
        display_field = field_mappings.get(field, "Name")
        linked_data[field] = {
            record["id"]: record["fields"].get(display_field, record["id"])
            for record in records
        }
        if records:
            print(f"Sample {table} record: {records[0]['fields']}")
            print(f"Total {table} records fetched: {len(records)}")

    return linked_data


def resolve_linked_fields(record_fields, linked_tables, linked_data, debug_label=None):
    """Resolve linked field IDs to their display values in a record.

    Args:
        record_fields: The record's fields dict (will be mutated).
        linked_tables: Dict of field names that may contain linked IDs.
        linked_data: The ID-to-value mappings from build_linked_data().
        debug_label: Optional label for debug printing (e.g. fund name).

    Returns:
        The mutated record_fields dict.
    """
    for field in linked_tables:
        if field in record_fields:
            value = record_fields[field]
            if isinstance(value, list):
                mapped_values = [
                    linked_data[field].get(id, id) for id in value
                    if id in linked_data[field]
                ]
                record_fields[field] = ", ".join(mapped_values) if mapped_values else ""
                if debug_label:
                    print(f"Transformed {field} for {debug_label}: {record_fields[field]}")
            else:
                record_fields[field] = linked_data[field].get(value, value) if value else ""

    return record_fields


def remove_unwanted_columns(record_fields, unwanted_columns):
    """Remove unwanted columns from a record's fields dict.

    Args:
        record_fields: The record's fields dict (will be mutated).
        unwanted_columns: List of column names to remove.

    Returns:
        The mutated record_fields dict.
    """
    for col in unwanted_columns:
        record_fields.pop(col, None)
    return record_fields


def write_records_to_csv(records, fieldnames, output_path):
    """Write a list of record dicts to a CSV file.

    Args:
        records: List of dicts to write.
        fieldnames: List of column names for the CSV header.
        output_path: File path to write the CSV to.
    """
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for record in records:
        writer.writerow(record)

    with open(output_path, "w") as f:
        f.write(output.getvalue())
