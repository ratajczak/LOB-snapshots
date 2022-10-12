FROM amazonlinux:2022

COPY aggregation_s3.py /opt/

CMD ["python3", "/opt/aggregation_s3.py"]