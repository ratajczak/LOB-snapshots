FROM amazonlinux:2022

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
#TODO requirements.txt
RUN pip3 install pandas numpy s3fs pyarrow orjson
COPY aggregation_s3.py /usr/src/app/