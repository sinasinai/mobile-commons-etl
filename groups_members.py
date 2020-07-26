"""API documentation can be found at: https://community.uplandsoftware.com/hc/en-us/articles/204494185-REST-API"""

import requests
import pandas as pd
import xmltodict
import json
import os
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
MC_USER = os.getenv("MC_USERNAME")
MC_PWD = os.getenv("MC_PASSWORD")
SCHEMA = os.getenv("SCHEMA")
TABLE_PREFIX = os.getenv("TABLE_PREFIX")

URL = "https://secure.mcommons.com/api/"
ALL_ENDPOINTS = ["group_members"]
INDEX_SET = {"groups": "group_id"}

RS_INCREMENTAL_KEYS = {"group_members": "updated_at", "groups": None}
API_INCREMENTAL_KEYS = {"group_members": "from", "groups": None}

ENDPOINT_KEY = {
    1: {"groups": "groups", "group_members": "group"},
    0: {"groups": "group", "group_members": "profile"},
}

MIN_PAGES = 1
MAX_PAGES = 20000
LIMIT = 500
AUTH = aiohttp.BasicAuth(MC_USER, password=MC_PWD)

# Mobile Commons API allows up to 160 concurrent connections but they asked us to reduce to 80 for now
SEMAPHORE = asyncio.BoundedSemaphore(80)

retries = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=1)
retry_adapter = HTTPAdapter(max_retries=retries)

http = requests.Session()
http.mount("https://secure.mcommons.com/api/", retry_adapter)


def main():

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
            "semaphore": SEMAPHORE,
            "auth": AUTH,
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
        template = pd.DataFrame(columns=tap.columns)
        df = pd.concat([template, data], sort=True, join="inner")

        print(
            "Loading data from endpoint {} into database...".format(
                str.upper(index), flush=True, file=sys.stdout
            )
        )

        tap.load(df, index)

        indices = set(data["id"])
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
                    "Kicking off extraction for endpoint {} GROUP {}...".format(
                        str.upper(ENDPOINT), i
                    ),
                    flush=True,
                    file=sys.stdout,
                )

                if subtap.page_count_get(**keywords, page=MIN_PAGES) > 0:

                    print("Guessing page count...")

                    subtap.page_count = subtap.get_page_count(**keywords)

                    print(
                        "There are {} pages in the result set for endpoint {} and GROUP {}".format(
                            subtap.page_count, str.upper(ENDPOINT), i
                        )
                    )

                    data = subtap.ping_endpoint(**keywords)
                    template = pd.DataFrame(columns=subtap.columns)

                    if data is not None:

                        df = pd.concat(
                            [template, data], sort=True, join="inner"
                        )
                        df[INDEX_SET[index]] = str(i)
                        index_results.append(df)

                else:

                    print(
                        "No new results to load for endpoint {} GROUP {}".format(
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
