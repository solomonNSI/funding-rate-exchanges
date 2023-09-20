import requests
import csv
from datetime import datetime

# Bybit API endpoint for funding rate history
url = "https://api.bybit.com/v5/market/funding/history"

# API Key and Secret
api_key = "insert your api key"
api_secret = "insert your secret key"

# Define the query parameters
params = {
    "category": "linear",
    "symbol": "ETHUSD",
}

# Add the API key to the headers
headers = {
    "api_key": api_key,
}

# Send a GET request to the Bybit API
response = requests.get(url, params=params, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()["result"]["list"]

    # Specify the CSV file name
    csv_file_name = "eth_funding_rate.csv"

    # Open the CSV file for writing
    with open(csv_file_name, 'w', newline='') as csvfile:
        # Create a CSV writer
        csv_writer = csv.writer(csvfile)

        # Define the header row
        header = ["symbol", "fundingRate", "fundingRateTimestamp", "fundingTimeDate"]
        csv_writer.writerow(header)

        # Write the data rows
        for row in data:
            # Convert the timestamp to a string date format
            timestamp_ms = int(row["fundingRateTimestamp"])  # Convert the string to an integer
            timestamp_sec = timestamp_ms / 1000  # Convert milliseconds to seconds
            date_str = datetime.utcfromtimestamp(timestamp_sec).strftime('%Y-%m-%d %H:%M:%S')

            # Append the date string to the row
            row["fundingTimeDate"] = date_str

            # Write the row to the CSV file
            csv_writer.writerow([row["symbol"], row["fundingRate"], row["fundingRateTimestamp"], row["fundingTimeDate"]])

    print(f"Funding rate data saved to {csv_file_name}")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
    print(response.text)