import okx.PublicData as PublicData
import csv
from datetime import datetime

flag = "0"  # Production trading: 0, Demo trading: 1

publicDataAPI = PublicData.PublicAPI(flag=flag)

# Define the instrument ID for the swap contract
instId = "ETH-USD"

# Specify the CSV file name
csv_file_name = "1.csv"

# Open the CSV file for writing
with open(csv_file_name, 'w', newline='') as csvfile:
    # Create a CSV writer
    csv_writer = csv.writer(csvfile)

    # Define the header row
    header = ["instType", "instId", "fundingRate", "realizedRate", "fundingTime", "fundingTimeDate"]
    csv_writer.writerow(header)

    before = 1686153600000  # Initialize before parameter
    limit = 100    # Number of results per request, maximum is 100

    while True:
        # Retrieve funding rate history
        if before == None: 
            result = publicDataAPI.funding_rate_history(instId=instId, limit=limit)
        else:
            result = publicDataAPI.funding_rate_history(instId=instId, before=before, limit=limit)

        # Check if the request was successful
        if result["code"] == "0":
            data = result["data"]

            # Break the loop if there are no more results
            if not data:
                break

            # Write the data rows
            for funding_data in data:
                timestamp_ms = int(funding_data["fundingTime"])  # Convert the string to an integer
                timestamp_sec = timestamp_ms / 1000  # Convert milliseconds to seconds
                date_str = datetime.utcfromtimestamp(timestamp_sec).strftime('%Y-%m-%d %H:%M:%S')

                # Write the row to the CSV file
                csv_writer.writerow([funding_data["instType"], funding_data["instId"], funding_data["fundingRate"],
                                     funding_data["realizedRate"], funding_data["fundingTime"], date_str])

            # Set 'before' parameter for the next request to get earlier records
            before = data[-1]["fundingTime"]

        else:
            print(f"Failed to fetch data. Code: {result['code']}, Message: {result['msg']}")
            break

print(f"Funding rate data saved to {csv_file_name}")
