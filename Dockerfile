FROM apache/airflow:2.11.0

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# добавляем JAR'ы для S3A
RUN mkdir -p /opt/airflow/jars
COPY jars/*.jar /opt/airflow/jars/

USER airflow

RUN  pip install \
      pyspark==3.5.5 \
      apache-airflow-providers-amazon \
      apache-airflow-providers-trino \
      apache-airflow-providers-postgres \
      apache-airflow-providers-apache-spark \
      boto3 \
      minio \
      soda-core-trino \
      pandas 

USER airflow
