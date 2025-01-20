FROM apache/airflow:latest
USER root
RUN apt update
RUN apt install -y default-jdk
COPY ./spark/spark /opt/spark

USER airflow
ENV JAVA_HOME='/usr/lib/jvm/java-1.17.0-openjdk-amd64'
ENV PATH="$PATH:/usr/lib/jvm/java-1.17.0-openjdk-amd64"
ENV PATH="$PATH:/opt/spark/bin:/opt/spark/sbin"
RUN pip install pyspark
RUN pip install apache-airflow-providers-apache-spark
ENV SPARK_HOME=/opt/spark


RUN pip install delta-spark