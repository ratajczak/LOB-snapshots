FROM amazonlinux:2022

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY aggregation_s3.py /usr/src/app/