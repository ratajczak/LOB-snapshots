FROM python:3.9-slim

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
#TODO requirements.txt
RUN pip3 install boto3 pandas numpy s3fs pyarrow orjson
COPY aggregation_s3.py /usr/src/app/