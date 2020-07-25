"""API documentation can be found at: https://community.uplandsoftware.com/hc/en-us/articles/204494185-REST-API"""

import requests
import pandas as pd
import xmltodict
import json
import os
import civis
import sys
import math
import datetime
import pytz
import dateparser
import asyncio
import aiohttp
import mobile_commons_etl as mc

from pandas.io.json import json_normalize
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

FULL_REBUILD_FLAG = os.getenv("FULL_REBUILD_FLAG")
CIVIS_API_KEY = os.getenv("CIVIS_API_KEY")
MC_USER = os.getenv("MC_USERNAME")
MC_PWD = os.getenv("MC_PASSWORD")
SCHEMA = os.getenv("SCHEMA")
TABLE_PREFIX = os.getenv("TABLE_PREFIX")


URL = "https://secure.mcommons.com/api/"
ALL_ENDPOINTS = ["messages"]
INDEX_SET = {"campaigns": "campaign_id"}

with open("./columns.json") as cols:
    COLUMNS = json.load(cols)

RS_INCREMENTAL_KEYS = {
    "sent_messages": "sent_at",
    "campaigns": None,
    "messages": "received_at",
}
API_INCREMENTAL_KEYS = {
    "sent_messages": "start_time",
    "campaigns": None,
    "messages": "start_time",
}
MASTER_CAMPAIGN_ID = "169115"

ENDPOINT_KEY = {
    1: {"campaigns": "campaigns", "sent_messages": "messages", "messages": "messages"},
    0: {"campaigns": "campaign", "sent_messages": "message", "messages": "message"},
}

MIN_PAGES = 1
MAX_PAGES = 20000
LIMIT = 500
AUTH = aiohttp.BasicAuth(MC_USER, password=MC_PWD)

# Mobile Commons API allows up to 160 concurrent connections but they asked us to reduce to 80 for now
SEMAPHORE = asyncio.BoundedSemaphore(80)

client = civis.APIClient()

retries = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=1)
retry_adapter = HTTPAdapter(max_retries=retries)

http = requests.Session()
http.mount("https://secure.mcommons.com/api/", retry_adapter)


def main():

    # SENT_MESSAGES endpoint is very slow, found a quicker workaround that
    # involves querying sent messages for each campaign  instead

    if str.lower(FULL_REBUILD_FLAG) == "true":

        full_build = True

    else:

        full_build = False

    for index in INDEX_SET.keys():

        keywords = {
            "session": http,
            "user": MC_USER,
            "pw": MC_PWD,
            "base": URL,
            "endpoint_key": ENDPOINT_KEY,
            "api_incremental_key": API_INCREMENTAL_KEYS[index],
            "limit": LIMIT,
            "min_pages": MIN_PAGES,
            "max_pages": MAX_PAGES,
            "columns": COLUMNS,
            "semaphore": SEMAPHORE,
            "auth": AUTH,
            "client": client,
            "schema": SCHEMA,
            "table_prefix": TABLE_PREFIX,
            "db_incremental_key": RS_INCREMENTAL_KEYS[index],
            "page_count": 1,
        }

        tap = mc.mobile_commons_connection(index, full_build, **keywords)
        tap.fetch_latest_timestamp()

        print(
            "Kicking off extraction for endpoint {}...".format(str.upper(index)),
            flush=True,
            file=sys.stdout,
        )

        data = tap.ping_endpoint(**keywords)
        template = pd.DataFrame(columns=COLUMNS[index], dtype="str")
        df = pd.concat([template, data.astype("str")], sort=True, join="inner")

        print(
            "Loading data from endpoint {} into database...".format(
                str.upper(index), flush=True, file=sys.stdout
            )
        )
        tap.load(df, index)

        indices = set(data["id"])
        indices = [str(c) for c in indices if str(c) != MASTER_CAMPAIGN_ID]

        index_results = []

        for i in indices:

            for ENDPOINT in ALL_ENDPOINTS:

                extrakeys = {
                    "api_incremental_key": API_INCREMENTAL_KEYS[ENDPOINT],
                    "db_incremental_key": RS_INCREMENTAL_KEYS[ENDPOINT],
                    INDEX_SET[index]: i,
                }
                keywords.update(extrakeys)
                subtap = mc.mobile_commons_connection(ENDPOINT, full_build, **keywords)
                subtap.index = INDEX_SET[index]
                subtap.fetch_latest_timestamp()

                print(
                    "Kicking off extraction for endpoint {} CAMPAIGN {}...".format(
                        str.upper(ENDPOINT), i
                    ),
                    flush=True,
                    file=sys.stdout,
                )

                ### Page count in results is given for messages endpoints, no need to guess

                subtap.page_count = subtap.page_count_get(**keywords, page=MIN_PAGES)

                if subtap.page_count > 0:

                    print(
                        "There are {} pages in the result set for endpoint {} and CAMPAIGN {}".format(
                            subtap.page_count, str.upper(ENDPOINT), i
                        )
                    )

                    data = subtap.ping_endpoint(**keywords)
                    template = pd.DataFrame(columns=COLUMNS[ENDPOINT], dtype="str")

                    if data is not None:

                        df = pd.concat(
                            [template, data.astype("str")], sort=True, join="inner"
                        )
                        df[INDEX_SET[index]] = str(i)
                        index_results.append(df)

                else:

                    print(
                        "No new results to load for endpoint {} CAMPAIGN {}".format(
                            str.upper(ENDPOINT), i
                        )
                    )

        all_results = pd.concat(index_results, sort=True, join="inner")

        print(
            "Loading data from endpoint {} into database...".format(
                str.upper(ENDPOINT), flush=True, file=sys.stdout
            )
        )

        subtap.load(all_results, ENDPOINT)


if __name__ == "__main__":

    main()
