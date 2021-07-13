import requests
import glob
import jsonlines
import logging
import time
from datetime import datetime
import sys
import argparse


MAX_LINES = 10000
logging.basicConfig(filename='log.txt')

def chunks(l, n):
    return [l[x: x+n] for x in range(0, len(l), n)]

def upload_ts(url, ts):
    try:
        requests.put(url, json=ts)
        time.sleep(0.2)
    except Exception as e:
        timestamp = datetime.utcnow()
        logging.error(f"{timestamp}: Uploading time series failed")
        logging.error(str(e))
        sys.exit(1)

def upload_data(data):
    if len(data) > MAX_LINES:
        data = chunks(data, len(data)//(len(data)//MAX_LINES))
        for chunk in data:
            upload_ts(tsdb, chunk)
    else:
        upload_ts(tsdb, data)

def parse_ts(metric, obj):
    return {
        "metric": metric,
        "timestamp": obj["TIMET"],
        "value": obj["SERVICESTATE"],
        "tags": {
            "host": obj["HOSTNAME"],
            "output": obj["SERVICEOUTPUT"],
            "type": obj["SERVICESTATETYPE"]
        }
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", type=str, dest="tsdb", help="url of tsdb server")
    args = vars(parser.parse_args())
    tsdb = args["tsdb"]

    for filename in glob.glob("icinga.*"):
        temp_data = []
        with jsonlines.open(filename) as f:
            for obj in f:
                temp_data.append(parse_ts(filename, obj))
        upload_data(temp_data)
