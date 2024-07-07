# -*- coding: utf-8 -*-

import requests

import json

import csv

import socket

import time
 
# Configuration data

config = {

    "symbols": ["AAPL", "IBM", "GOOGL", "MSFT", "TSLA"],

    "api_key": "EYIN6VX7OOQO5AK8",

    "url": "https://www.alphavantage.co/query",

    "port": 12345,

    "max_line_length": 511,

}
 
# Function to send data to the specified port in chunks

def send_data_to_port(data, port, max_line_length):

    data_str = json.dumps(data)

    print "Data length:", len(data_str)

    chunks = [data_str[i:i + max_line_length] for i in range(0, len(data_str), max_line_length)]

    print "Total chunks: %d" % len(chunks)
 
    updateSocket = socket.socket()

    host = socket.gethostname()

    port = 12345

    updateSocket.connect((host, port))

    for chunk in chunks:

        print "Sending data chunk to localhost:%s" % port

        print "Chunk length:", len(chunk)

        updateSocket.send(chunk + "\n")

        print "Data sent successfully"

    updateSocket.close()
 
# Function to convert JSON data to CSV format

def convert_to_csv(data):

    csv_data = []

    for key, value in data.items():

        if isinstance(value, dict):

            row = [key] + value.values()

            csv_data.append(row)

    return csv_data
 
# Function to fetch stock data and send it to HDFS

def fetch_stock_data():

    api_key = 'EYIN6VX7OOQO5AK8'
 
    for symbol in config["symbols"]:

        api_url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=%s&interval=1min&outputsize=full&apikey=%s" % (symbol, api_key)
 
        try:

            response = requests.get(api_url)
 
            if response.status_code == 200:

                data = response.json()
 
                # Print the first 10 lines of data received

                print "First 10 lines of data received for", symbol

                for line in json.dumps(data, indent=2).split('\n')[:10]:

                    print line
 
                # Convert data to CSV format

                csv_data = convert_to_csv(data)
 
                # Print the first 10 lines of converted data

                print "\nFirst 10 lines of converted data for", symbol

                for row in csv_data[:10]:

                    print row
 
                # Send data in chunks to port

                send_data_to_port(csv_data, config["port"], config["max_line_length"])
 
                # Data sent notification

                print "Data sent to port, now waiting for 60 seconds!"

                time.sleep(60)  # Wait for 1 minute
 
            else:

                print "Failed to fetch data for symbol", symbol

        except Exception as e:

            print "Error fetching data for symbol", symbol, ":", e
 
if __name__ == '__main__':

    fetch_stock_data()
 

