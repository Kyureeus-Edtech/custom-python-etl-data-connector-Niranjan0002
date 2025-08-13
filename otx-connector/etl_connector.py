import os
import requests
import pymongo
from dotenv import load_dotenv
from datetime import datetime
import time

# Load environment variables
load_dotenv()

OTX_API_KEY = os.getenv("OTX_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

BASE_URL = "https://otx.alienvault.com/api/v1/indicators/IPv4/{ip}/general"

def extract(ip):
    """Extract IOC details from OTX API for a given IP."""
    headers = {"X-OTX-API-KEY": OTX_API_KEY}
    url = BASE_URL.format(ip=ip)
    print(f"[INFO] Fetching data for IP: {ip}")
    resp = requests.get(url, headers=headers, timeout=10)
    
    if resp.status_code == 404:
        print(f"[WARN] No data found for IP: {ip}")
        return None
    resp.raise_for_status()
    return resp.json()

def transform(data, ip):
    """Transform raw API data by adding metadata."""
    if not data:
        return None
    transformed = {
        "ip": ip,
        "data": data,
        "ingested_at": datetime.utcnow()
    }
    return transformed

def load(data):
    """Load transformed data into MongoDB."""
    if not data:
        return
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    collection.insert_one(data)
    client.close()
    print(f"[INFO] Inserted data for IP: {data['ip']} into MongoDB.")

if __name__ == "__main__":
    # Example IP list (replace or extend as needed)
    ip_list = ["8.8.8.8", "1.1.1.1", "208.67.222.222", "208.67.220.220"]



    for ip in ip_list:
        try:
            raw_data = extract(ip)
            transformed_data = transform(raw_data, ip)
            load(transformed_data)
            time.sleep(1)  # Avoid hitting rate limits
        except Exception as e:
            print(f"[ERROR] Failed for {ip}: {e}")
