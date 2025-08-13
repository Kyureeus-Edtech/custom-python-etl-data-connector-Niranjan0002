import os
import requests
import pymongo
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

OTX_API_KEY = os.getenv("OTX_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

BASE_URL = "https://otx.alienvault.com/api/v1/indicators/IPv4/{ip}/general"

def extract(ip):
    headers = {"X-OTX-API-KEY": OTX_API_KEY}
    url = BASE_URL.format(ip=ip)
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()

def transform(data):
    # Add ingestion timestamp
    data["ingested_at"] = datetime.utcnow()
    return data

def load(data):
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    collection.insert_one(data)
    client.close()

if __name__ == "__main__":
    ip_list = ["8.8.8.8", "1.1.1.1"]  # Example IPs
    for ip in ip_list:
        try:
            raw_data = extract(ip)
            transformed_data = transform(raw_data)
            load(transformed_data)
            print(f"Data for {ip} inserted successfully.")
        except Exception as e:
            print(f"Error processing {ip}: {e}")
