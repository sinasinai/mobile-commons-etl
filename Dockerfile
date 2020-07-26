FROM civisanalytics/datascience-python:latest


RUN pip install xmltodict
RUN pip install dateparser
RUN pip install asyncio
RUN pip install aiohttp
RUN pip install ipdb
RUN pip install sqlalchemy


# CMD /bin/bash -c "source vars.sh && python all_messages.py true"
