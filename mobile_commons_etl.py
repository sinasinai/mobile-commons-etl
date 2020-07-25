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
import numpy as np

from pandas.io.json import json_normalize


class mobile_commons_connection:
    def __init__(self, endpoint, full_build, **kwargs):

        self.endpoint = endpoint
        self.full_build = full_build
        self.semaphore = kwargs.get("semaphore", None)
        self.auth = kwargs.get("auth", None)
        self.limit = kwargs.get("limit", None)
        self.base = kwargs.get("base", None)
        self.endpoint_key = kwargs.get("endpoint_key", None)
        self.columns = kwargs.get("columns", None)
        self.page_count = kwargs.get("page_count", None)
        self.session = kwargs.get("session", None)
        self.user = kwargs.get("user", None)
        self.pw = kwargs.get("pw", None)
        self.base = kwargs.get("base", None)
        self.api_incremental_key = kwargs.get("api_incremental_key", None)
        self.min_pages = kwargs.get("min_pages", None)
        self.max_pages = kwargs.get("max_pages", None)
        self.last_timestamp = kwargs.get("last_timestamp", None)
        self.load_client = kwargs.get("client", None)
        self.group_id = kwargs.get("group_id", None)
        self.campaign_id = kwargs.get("campaign_id", None)
        self.url_id = kwargs.get("url_id", None)
        self.index = None
        self.index_id = self.group_id or self.url_id or self.campaign_id
        self.db_incremental_key = kwargs.get("db_incremental_key", None)
        self.schema = kwargs.get("schema", "public")
        self.table_prefix = kwargs.get("table_prefix", "")

    async def get_page(self, page, retries=5, **kwargs):
        """Base asynchronous request function"""

        params = {"page": page}

        if self.group_id is not None:
            params["group_id"] = self.group_id

        if self.campaign_id is not None:
            params["campaign_id"] = self.campaign_id

        if (self.api_incremental_key is not None) & (self.last_timestamp is not None):
            params[self.api_incremental_key] = self.last_timestamp

        if self.url_id is not None:
            params["url_id"] = self.url_id

        if self.limit is not None:
            params["limit"] = self.limit

        url = f"{self.base}{self.endpoint}"
        print(f"Fetching page {page}")

        TIMEOUT = aiohttp.ClientTimeout(total=60 * 60)

        attempts = 1
        data = None

        while data is None or attempts <= retries:

            try:
                async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
                    async with self.semaphore, session.get(
                        url, params=params, auth=self.auth
                    ) as resp:
                        resp.raise_for_status()
                        print(f"{resp.url} status: {resp.status}")
                        data = await resp.text()
                        return data

            except aiohttp.ClientError:
                print(f"Retrying {page}...")
                attempts += 1
                await asyncio.sleep(1)

    def ping_endpoint(self, **kwargs):
        """Wrapper for asynchronous calls that then have results collated into a dataframe"""

        loop = asyncio.get_event_loop()

        # Chunks async calls into bundles if page count is greater than 500

        if self.page_count > 500:
            partition_size = int(
                math.ceil(
                    self.page_count ** (1 - math.log(500) / math.log(self.page_count))
                )
            )
            breaks = [
                int(n)
                for n in np.linspace(1, self.page_count + 1, partition_size).tolist()
            ]
        else:
            breaks = [1, self.page_count + 1]

        res = []

        for b in range(1, len(breaks)):

            temp = loop.run_until_complete(
                asyncio.gather(
                    *(
                        self.get_page(page, **kwargs)
                        for page in range(breaks[b - 1], breaks[b])
                    )
                )
            )
            res += temp

        endpoint_key_0 = self.endpoint_key[0][self.endpoint]
        endpoint_key_1 = self.endpoint_key[1][self.endpoint]

        res_list = []

        for r in res:
            try:
                if (
                    json.loads(json.dumps(xmltodict.parse(r))).get("response")
                    is not None
                ):
                    json_xml = json.loads(json.dumps(xmltodict.parse(r)))
                    page_result = json_normalize(
                        json_xml["response"][endpoint_key_1][endpoint_key_0]
                    )
                    res_list.append(page_result)
            except:
                print("Improperly formatted XML response... skipping")
                continue

        df_agg = pd.concat(res_list, sort=True, join="outer")
        df_agg.columns = [
            c.replace(".", "_").replace("@", "").replace("@_", "").replace("_@", "")
            for c in df_agg.columns
        ]
        df_agg = df_agg.loc[:, df_agg.columns.isin(self.columns[self.endpoint])]
        return df_agg

    def get_latest_record(self, endpoint):
        """Pulls the latest record from the database to use for incremental updates"""

        table = f"{self.schema}.{self.table_prefix}_{endpoint}"

        index_filter = """"""
        if self.index is not None:
            index_filter = (
                """ where """ + self.index + """::integer = """ + str(self.index_id)
            )

        sql = (
            """select max(case when """
            + self.db_incremental_key
            + """ = 'None' then null else """
            + self.db_incremental_key
            + """::timestamp end) as latest_date from {}""".format(table)
            + index_filter
        )

        date = civis.io.read_civis_sql(sql, "TMC", use_pandas=True)

        if date.shape[0] > 0:
            latest_date = date["latest_date"][0]
            utc = pytz.timezone("UTC")
            dateparser.parse(latest_date).astimezone(utc).isoformat()
        else:
            latest_date = None

        return latest_date

    def fetch_latest_timestamp(self):
        """Handler for pulling latest record if incremental build"""

        if (not self.full_build) & (self.db_incremental_key is not None):

            print(
                "Getting latest record for endpoint {}...".format(
                    str.upper(self.endpoint)
                ),
                flush=True,
                file=sys.stdout,
            )
            self.last_timestamp = self.get_latest_record(self.endpoint)
            print(f"Latest timestamp: {self.last_timestamp}")

        else:
            self.last_timestamp = None
            self.full_build = False

        return self.last_timestamp

    def page_count_get(self, page, **kwargs):
        """
        Returns metadata around whether a request has non-zero result set
        since endpoints are paginated but page count isn't always given
        """

        endpoint_key_0 = self.endpoint_key[0][self.endpoint]
        endpoint_key_1 = self.endpoint_key[1][self.endpoint]

        params = {"page": page}

        if self.limit is not None:
            params["limit"] = self.limit

        if (self.api_incremental_key is not None) & (self.last_timestamp is not None):
            params[self.api_incremental_key] = self.last_timestamp

        if self.group_id is not None:
            params["group_id"] = self.group_id

        if self.campaign_id is not None:
            params["campaign_id"] = self.campaign_id

        if self.url_id is not None:
            params["url_id"] = self.url_id

        resp = self.session.get(
            self.base + self.endpoint, auth=(self.user, self.pw), params=params
        )

        print(f"{resp.url}")

        formatted_response = json.loads(json.dumps(xmltodict.parse(resp.text)))[
            "response"
        ][endpoint_key_1]

        ### Sina: 7/20/20 only broadcasts, messages, & sent_messages endpoints have page_count field this at this point in time

        if formatted_response.get("@page_count") is not None:
            num_results = int(formatted_response.get("@page_count"))

        elif formatted_response.get("page_count") is not None:
            num_results = int(formatted_response.get("page_count"))

        elif formatted_response.get(endpoint_key_0) is not None:
            num_results = 1

        else:
            num_results = 0

        return num_results

    def get_page_count(self, **kwargs):
        """Binary search to find page count for an endpoint if page count data isn't available so we can parallelize downstream"""

        guess = math.floor((self.min_pages + self.max_pages) / 2)
        diff = self.max_pages - self.min_pages

        print(f"Page count guess: {guess}")
        kwargs["page"] = guess

        if (self.page_count_get(**kwargs) > 0) & (diff > 1):
            self.min_pages = guess
            return self.get_page_count(**kwargs)

        elif (self.page_count_get(**kwargs) == 0) & (diff > 1):
            self.max_pages = guess
            return self.get_page_count(**kwargs)

        else:
            print(f"Page count converged! Final count: {guess}")
            return guess

    def load(self, df, endpoint):
        """Loads to database"""

        if self.full_build:
            civis.io.dataframe_to_civis(
                df.astype(str),
                "TMC",
                f"{self.schema}.{self.table_prefix}_{endpoint}",
                client=self.load_client,
                existing_table_rows="drop",
                table_columns=[
                    {"name": c, "sqlType": "varchar(65535)"}
                    for c in self.columns[self.endpoint]
                ],
            ).result()

        else:
            civis.io.dataframe_to_civis(
                df.astype(str),
                "TMC",
                f"{self.schema}.{self.table_prefix}_{endpoint}",
                client=self.load_client,
                existing_table_rows="append",
                table_columns=[
                    {"name": c, "sqlType": "varchar(65535)"}
                    for c in self.columns[self.endpoint]
                ],
            ).result()
