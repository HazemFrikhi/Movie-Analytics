# Use the official Airflow image as the base
FROM apache/airflow:latest

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Install necessary packages and Spark dependencies
USER root
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark