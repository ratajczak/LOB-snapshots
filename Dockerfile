FROM amazonlinux:2022

COPY aggregation.py .

CMD ["python3", "aggregation.py"]