FROM civisanalytics/datascience-python:latest

ADD . .

RUN pip install xmltodict
RUN pip install dateparser
RUN pip install asyncio
RUN pip install aiohttp

# CMD /bin/bash -c "source vars.sh && python all_messages.py true"
