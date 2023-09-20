import asyncio
import websockets
import json
import csv
from datetime import datetime

async def call_api(msg):
    async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
        await websocket.send(msg)
        data = []

        while websocket.open:
            response = await websocket.recv()
            response_data = json.loads(response)

            if "result" in response_data:
                data.extend(response_data["result"])

            if "error" in response_data:
                print(f"Error: {response_data['error']}")

            if "id" in response_data and response_data["id"] == 7617:
                break

        return data

async def main():
    msg = {
        "jsonrpc": "2.0",
        "id": 7617,
        "method": "public/get_funding_rate_history",
        "params": {
            "instrument_name": "ETH-PERPETUAL",
            "start_timestamp": 1569888000000,
            "end_timestamp": 1694685698000,
            "grant_type" : "client_credentials",
            "client_id" : "HIHnZucV",
            "client_secret" : "b7le00DwVThHAjt0GMOrDLGyYTjNNO6SzlW5R9v8pIg"
        }
    }
    count = 0
    
    while msg["params"]["start_timestamp"] != msg["params"]["end_timestamp"]:
        
        data = await call_api(json.dumps(msg))

        if data:
            # Specify the CSV file name
            csv_file_name = "why" + str(count) + ".csv"
            count = count + 1

            # Open the CSV file for writing
            with open(csv_file_name, 'w', newline='') as csvfile:
                # Create a CSV writer
                csv_writer = csv.writer(csvfile)

                # Define the header row
                header = ["time", "timestamp", "index_price", "prev_index_price", "interest_8h", "interest_1h"]
                csv_writer.writerow(header)

                # Write the data rows
                count1 = 0
                for funding_data in data:
                    # print(funding_data)
                    if count1 == 0:
                        msg["params"]["end_timestamp"] = funding_data["timestamp"]
                    count1 = count1 + 1
                    timestamp_ms = funding_data["timestamp"]
                    timestamp_sec = timestamp_ms / 1000  # Convert milliseconds to seconds
                    date_str = datetime.utcfromtimestamp(timestamp_sec).strftime('%Y-%m-%d %H:%M:%S')

                    # Write the row to the CSV file
                    csv_writer.writerow([date_str, funding_data["timestamp"], funding_data["index_price"], funding_data["prev_index_price"],
                                        funding_data["interest_8h"], funding_data["interest_1h"]])

            print(f"Funding rate data saved to {csv_file_name}")

asyncio.get_event_loop().run_until_complete(main())
