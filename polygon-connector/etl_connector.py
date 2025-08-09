#!/usr/bin/env python3
"""
etl_connector.py
Polygon -> MongoDB ETL connector script.

Usage example:
  python etl_connector.py --ticker AAPL --from 2024-07-01 --to 2024-07-07
"""

import os
import time
import argparse
import logging
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, errors

# Load environment variables from .env
load_dotenv()

API_KEY = os.getenv("POLYGON_API_KEY")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "etl_db")
BASE_URL = "https://api.polygon.io"

# Retry and rate limit configs
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
RATE_LIMIT_SLEEP = 12  # seconds (to respect free tier 5 req/min limit)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def request_with_retries(url, params=None, headers=None, max_retries=MAX_RETRIES):
    params = params or {}
    headers = headers or {}
    params["apiKey"] = API_KEY
    backoff = 1
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
        except requests.RequestException as e:
            logging.warning(f"Request exception (attempt {attempt}/{max_retries}): {e}")
            time.sleep(backoff)
            backoff *= BACKOFF_FACTOR
            continue

        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            logging.warning(f"Rate limited (429). Sleeping {RATE_LIMIT_SLEEP}s.")
            time.sleep(RATE_LIMIT_SLEEP)
        elif 500 <= resp.status_code < 600:
            logging.warning(f"Server error {resp.status_code}. Backing off {backoff}s.")
            time.sleep(backoff)
            backoff *= BACKOFF_FACTOR
        else:
            logging.error(f"Request failed {resp.status_code}: {resp.text[:300]}")
            resp.raise_for_status()
    raise RuntimeError(f"Max retries exceeded for URL: {url}")


def fetch_aggs(ticker, from_date, to_date, multiplier=1, timespan="day"):
    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
    logging.info(f"Fetching aggregates for {ticker} from {from_date} to {to_date}")
    data = request_with_retries(url)
    results = data.get("results", [])
    next_url = data.get("next_url")
    while next_url:
        logging.info("Fetching next page of results.")
        data2 = request_with_retries(next_url)
        results.extend(data2.get("results", []))
        next_url = data2.get("next_url")
    return results


def transform_aggs(results, ticker):
    docs = []
    for item in results:
        ts_ms = item.get("t")
        if ts_ms is None:
            continue
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        doc = {
            "ticker": ticker,
            "date": dt,
            "open": item.get("o"),
            "high": item.get("h"),
            "low": item.get("l"),
            "close": item.get("c"),
            "volume": item.get("v"),
            "raw": item,  # keep raw payload
            "ingested_at": datetime.utcnow(),
        }
        docs.append(doc)
    return docs


def load_to_mongo(docs, collection_name, mongo_uri=MONGO_URI, db_name=DB_NAME, batch_size=500):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    coll = db[collection_name]
    try:
        coll.create_index([("ticker", 1), ("date", 1)], unique=True)
    except errors.PyMongoError as e:
        logging.warning(f"Could not create index: {e}")

    total = 0
    for i in range(0, len(docs), batch_size):
        batch = docs[i : i + batch_size]
        if not batch:
            continue
        try:
            result = coll.insert_many(batch, ordered=False)
            total += len(result.inserted_ids)
        except errors.BulkWriteError as bwe:
            logging.warning(f"Bulk write error: {bwe.details.get('writeErrors', [])[:2]}")
        except errors.DuplicateKeyError:
            logging.info("Duplicate key error encountered, skipping duplicates.")
        except Exception as e:
            logging.error(f"Error during insert: {e}")
    logging.info(f"Inserted approx {total} new documents into {db_name}.{collection_name}")
    client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", required=True, help="Ticker symbol (e.g., AAPL)")
    parser.add_argument("--from", dest="from_date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--db", dest="db_name", default=DB_NAME)
    args = parser.parse_args()

    ticker = args.ticker.upper()
    from_date = args.from_date
    to_date = args.to_date

    raw_results = fetch_aggs(ticker, from_date, to_date)
    if not raw_results:
        logging.warning(f"No results returned for {ticker} between {from_date} and {to_date}")
        return

    docs = transform_aggs(raw_results, ticker)
    collection_name = f"polygon_{ticker}_raw"
    load_to_mongo(docs, collection_name, db_name=args.db_name)


if __name__ == "__main__":
    if not API_KEY:
        raise RuntimeError("POLYGON_API_KEY not set in environment (.env).")
    main()
