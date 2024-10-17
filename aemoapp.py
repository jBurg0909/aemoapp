from flask import Flask, jsonify, make_response
import requests
import pandas as pd
from bs4 import BeautifulSoup
import io
import os
import zipfile

aemoapp = Flask(__name__)

@aemoapp.route("/")
def index():
    return "Hello, I'm Jack"

@aemoapp.route("/<name>")
def print_name(name):
    return f"Hi {name}"

# Base URL for NEM Reports
BASE_URL = "https://nemweb.com.au/Reports/Current/Operational_Demand/FORECAST_HH"

def fetch_file_links():
    """
    Fetches all .zip file links from the FORECAST_HH folder.
    """
    try:
        response = requests.get(BASE_URL)
        response.raise_for_status()  # Raise exception for non-200 responses
        soup = BeautifulSoup(response.text, "html.parser")
        
        zip_files = []
        for link in soup.find_all("a"):
            href = link.get("href")
            if href.endswith(".zip"):
                zip_files.append(BASE_URL + "/" + os.path.basename(href))
        
        return zip_files
    except requests.RequestException as e:
        print(f"Error fetching file links: {e}")
        return []

def download_and_extract_csv(zip_url):
    """
    Downloads a zip file from a URL and extracts the first .csv file.
    Returns the DataFrame of the extracted .csv file.
    """
    try:
        zip_response = requests.get(zip_url)
        zip_response.raise_for_status()

        if 'application/x-zip-compressed' in zip_response.headers['Content-Type']:
            with zipfile.ZipFile(io.BytesIO(zip_response.content)) as thezip:
                csv_filename = thezip.namelist()[0]  # Get the first .csv file
                with thezip.open(csv_filename) as thecsv:
                    df = pd.read_csv(thecsv)
                    return df
        else:
            print("Unexpected Content-Type:", zip_response.headers['Content-Type'])
            return None
    except requests.RequestException as e:
        print(f"Error downloading or extracting CSV: {e}")
        return None

@aemoapp.route('/api/data', methods=['GET'])
def get_aemo_data():
    """
    API endpoint to fetch the latest operational demand forecast data.
    Returns the data in JSON format.
    """
    zip_files = fetch_file_links()
    
    if not zip_files:
        return make_response(jsonify({"error": "No zip files found or failed to retrieve them"}), 500)
    
    # Fetch the latest zip file
    latest_zip = zip_files[-1]

    # Extract the CSV from the latest zip file
    df = download_and_extract_csv(latest_zip)
    
    if df is not None:
        # Convert DataFrame to JSON
        json_data = df.to_json(orient='records', lines=True)
        return make_response(json_data, 200)
    else:
        return make_response(jsonify({"error": "Failed to download or extract CSV data"}), 500)

if __name__ == '__main__':
    aemoapp.run(debug=True)