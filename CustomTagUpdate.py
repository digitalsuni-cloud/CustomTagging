import requests
import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define command line arguments
parser = argparse.ArgumentParser(description='MultiThread Tasks from a CSV')
parser.add_argument('--api_key', required=True)
parser.add_argument('--filename', required=True)
args = parser.parse_args()

api_key = args.api_key
csv_filename = args.filename

def process_line(l):
    cloud = l.pop(0).lower()
    account_id = l.pop(0)
    tag_list = l

    if cloud == 'gcp':
        search_url = (f"https://chapi.cloudhealthtech.com/api/search?api_key={api_key}&api_version=2&page=1&name=GcpComputeProject&query=name='{account_id}'&fields=name")
        asset_type = 'GcpComputeProject'
    elif cloud == 'aws':
        search_url = (f"https://chapi.cloudhealthtech.com/api/search?api_key={api_key}&api_version=2&page=1&name=AwsAccount&query=owner_id='{account_id}'&fields=name")
        asset_type = 'AwsAccount'
    elif cloud == 'azr':
        search_url = (f"https://chapi.cloudhealthtech.com/api/search?api_key={api_key}&api_version=2&page=1&name=AzureSubscription&query=azure_id='{account_id}'&fields=name")
        asset_type = 'AzureSubscription'
    else:
        print(f"[ERROR] Unsupported cloud type: {cloud}")
        return None

    response = requests.get(search_url)
    json_data = response.json()
    cloudhealth_id = json_data[0]["id"] if json_data else None

    # Build tags list correctly
    tags = []
    for i in range(0, len(tag_list), 2):
        key = tag_list[i]
        value = tag_list[i+1]
        if value == "null":
            tags.append({"key": key, "value": None})
        elif value != "":
            tags.append({"key": key, "value": value})

    # Ensure ids is always a list, empty if no valid id
    ids = [cloudhealth_id] if cloudhealth_id else []

    payload = {
        "tag_groups": [
            {
                "asset_type": asset_type,
                "ids": ids,
                "tags": tags
            }
        ]
    }
    post_url = "https://chapi.cloudhealthtech.com/v1/custom_tags"
    params = {"api_key": api_key}

    # Log request details
    print(f"[LOG] Setting tags for asset_type: {asset_type} | ids: {ids} | tags: {tags}")

    tag_response = requests.post(post_url, json=payload, params=params, headers={"Content-Type": "application/json"})

    # Log response status and content
    print(f"[LOG] Response code: {tag_response.status_code} | Response text: {tag_response.text}")

    return tag_response.text

processes = []

with ThreadPoolExecutor(max_workers=8) as executor:
    with open(csv_filename, encoding='utf-8-sig') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        header = next(readCSV, None)  # Skip header row
        for line in readCSV:
            if line:
                processes.append(executor.submit(process_line, line))

for task in as_completed(processes):
    print(task.result())
