import csv
import logging
import json
from datetime import datetime
from binance.cm_futures import CMFutures
from binance.lib.utils import config_logging

def append_json_to_csv(json_response, csv_file_name):
    # Check if the CSV file already exists
    file_exists = False
    try:
        with open(csv_file_name, 'r') as csvfile:
            file_exists = True
    except FileNotFoundError:
        file_exists = False

    # Open the CSV file in append mode if it exists, or create a new file if it doesn't
    with open(csv_file_name, 'a', newline='') as csvfile:
        # Create a CSV writer
        csv_writer = csv.writer(csvfile)

        # If the file doesn't exist, write the header row
        if not file_exists:
            header = ["symbol", "fundingTime", "fundingRate", "fundingTimeDate"]
            csv_writer.writerow(header)

        # Write the data rows
        for row in json_response:
            # Convert the timestamp to a string date format
            timestamp_ms = row["fundingTime"]
            timestamp_sec = timestamp_ms / 1000  # Convert milliseconds to seconds
            date_str = datetime.utcfromtimestamp(timestamp_sec).strftime('%Y-%m-%d %H:%M:%S')

            # Append the date string to the row
            row["fundingTimeDate"] = date_str

            # Write the row to the CSV file
            csv_writer.writerow([row["symbol"], row["fundingTime"], row["fundingRate"], row["fundingTimeDate"]])

config_logging(logging, logging.DEBUG)

key = 'INSERT_YOUR_KEY'

# historical_trades requires api key in request header
cm_futures_client = CMFutures(key=key)

data = cm_futures_client.funding_rate("BTCUSD_PERP", **{
    "startTime" : 
    "limit": 1000
    })

csv_file_name = "output_with_date.csv"

# Append data from json_response1
append_json_to_csv(data, csv_file_name)

