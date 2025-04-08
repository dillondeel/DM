import requests
import csv
import io
import os

# Airtable setup
url = "https://api.airtable.com/v0/appQY8Pco55RA0JSp/Fundraising%20Rounds%20-%20Companies"
headers = {"Authorization": f"Bearer {os.getenv('AIRTABLE_API_KEY')}"}

# Fetch all records with pagination
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

# Extract fields
fields = set()
for record in all_records:
    fields.update(record["fields"].keys())
fields = list(fields)

# Convert to CSV
output = io.StringIO()
writer = csv.DictWriter(output, fieldnames=fields)
writer.writeheader()
for record in all_records:
    writer.writerow(record["fields"])

# Write to file
with open("data/fundraising_rounds_companies.csv", "w") as f:
    f.write(output.getvalue())
