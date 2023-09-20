import requests
import csv
from datetime import datetime

# Bitget API endpoint for funding rate history
url = "https://api.bitget.com/api/mix/v1/market/history-fundRate"

# Define the query parameters
params = {
    "symbol": "ETHUSD_DMCBL",
    "pageSize": 100,  # Adjust the page size as needed
    "pageNo": 1,  # Start with page 1
}

# Specify the CSV file name
csv_file_name = "bitget_eth_funding_rate.csv"

# Open the CSV file for writing
with open(csv_file_name, 'w', newline='') as csvfile:
    # Create a CSV writer
    csv_writer = csv.writer(csvfile)

    # Define the header row
    header = ["symbol", "fundingRate", "settleTime", "settleTimeDate"]
    csv_writer.writerow(header)

    while True:
        # Send a GET request to the Bitget API with the current parameters
        response = requests.get(url, params=params)

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()["data"]

            # Break the loop if there are no more results
            if not data:
                break

            # Write the data rows
            for funding_data in data:
                timestamp_ms = int(funding_data["settleTime"])  # Convert the string to an integer
                timestamp_sec = timestamp_ms / 1000  # Convert milliseconds to seconds
                date_str = datetime.utcfromtimestamp(timestamp_sec).strftime('%Y-%m-%d %H:%M:%S')

                # Write the row to the CSV file
                csv_writer.writerow([funding_data["symbol"], funding_data["fundingRate"], funding_data["settleTime"], date_str])

            # Increment the page number for the next request
            params["pageNo"] += 1
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            print(response.text)
            break

print(f"All funding rate data saved to {csv_file_name}")
