FROM amazonlinux:2022

COPY aggregation_s3.py .

CMD ["python3", "aggregation_s3.py"]