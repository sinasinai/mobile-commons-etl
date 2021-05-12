# Mobile Commons ETL Repo

A set of ETL scripts & utility functions based on the [API documentation](https://community.uplandsoftware.com/hc/en-us/articles/204494185-REST-API) that uses the [AsyncIO library](https://docs.python.org/3/library/asyncio.html) to load the extracted data into a (in this case Redshift) warehouse using the [Civis API](https://civis-python.readthedocs.io/en/stable/). This can easily be modified to use other clients.

Start up Bash shell in Docker container by typing `docker-compose run etl` in Terminal, otherwise run scripts how you see fit e.g. `python profiles.py`. I schedule these in a DAG using Civis Workflows, though you can easily do the same using Airflow or some other task scheduler (which is also included here).

Getting started:
ACLU instructions:
1. Add personal values to src/env.list
2. edit the "volumes" in docker-compose.yml to match your local install of this repo
Run the following while in `/src`:

1. `docker-compose build`
2. `docker-compose run --service-ports etl`

You should now be in a running container! If you'd like to run a local Dockerized Airflow instance, make sure to run the following commands:

1. (Optional) `export AIRFLOW_HOME="/airflow"`
2. `airflow initdb` to initialize the Airflow SQLite DB
3. `airflow webserver &` (by default to port 8080)
4. `airflow scheduler &` to kick off the scheduler
5. Navigate to `localhost:8080` to find the Airflow dashboard!

I've had issues running the webserver & scheduler in daemon mode before, and have resorted to bashing in a separate session to the container running `docker exec -ti <container_id> bash` and running the remaining commands. Airflow has great [docs](https://airflow.apache.org/docs/stable/start.html) and I've included my very simple DAG & config files for reference.


### Environmental Variables

`FULL_REBUILD_FLAG` - String, value "True" or "False". Determines whether a full or incremental extraction is run based on latest timestamp in the warehouse.

`CIVIS_API_KEY` = String, Civis API key, not necessary if you use to use another client.

`MC_USER` = String, Mobile Commons username/email.

`MC_PASSWORD` = String, Mobile Commons password.

`SCHEMA` = String, target warehouse schema you're loading data to.

`TABLE_PREFIX` = Table name prepend you'd like to affix to the tables you're loading. E.g. If your `TABLE_PREFIX` is `mobile_commons`, the table name loaded will be `{SCHEMA}.mobile_commons_{table}`.

### Miscellaneous

`columns.json` - Dict, Contains a pre-mapped set of columns to load into the warehouse post-processing of XML responses from the Mobile Commons endpoints & ensure consistency.

`mobile_commons_etl.get_page_count()` - Function, used to determine the total number of pages a query results in when page count values in response are not available.

The `sent_messages` endpoint is notoriously slow, and I've opted to extract messages by looping & filtering by campaign as that seems to speed up the performance. `MASTER_CAMPAIGN_ID` is hardcoded at the top of the scripts for exclusion since the Master Campaign is an aggregate of the other campaigns, but this can be converted to an environmental variable as you all see fit.
