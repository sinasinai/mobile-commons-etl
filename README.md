# Mobile Commons ETL Repo

A set of ETL scripts & utility functions based on the [API documentation](https://community.uplandsoftware.com/hc/en-us/articles/204494185-REST-API) that uses the [AsyncIO library](https://docs.python.org/3/library/asyncio.html) to load the extracted data into a (in this case Redshift) warehouse using the [Civis API](https://civis-python.readthedocs.io/en/stable/). This can easily be modified to use other clients.

### Environmental Variables

`FULL_REBUILD_FLAG` - String, value "True" or "False". Determines whether a full or incremental extraction based on latest timestamp in the warehouse is performed.

`CIVIS_API_KEY` = String, Civis API key, not necessary if your repurpose to use another client.

`MC_USER` = String, Mobile Commons username/email.

`MC_PWD` = String, Mobile Commons password.

`SCHEMA` = String, target warehouse schema you're loading data to.

`TABLE_PREFIX` = Table name prepend you'd like to affix to the tables you're loading. E.g. If your `TABLE_PREFIX` is `mobile_commons`, the table name loaded will be `{SCHEMA}.mobile_commons_{table}`
