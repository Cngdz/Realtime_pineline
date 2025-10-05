FROM bitnamilegacy/spark:3.3

USER root

COPY requirements.txt /app/


RUN apt-get update && apt-get install -y python3-pip && \
    pip install --no-cache-dir -r /app/requirements.txt

USER 1001