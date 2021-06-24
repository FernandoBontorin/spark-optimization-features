FROM apache/airflow:2.0.2
USER root
RUN apt update
RUN apt install --no-install-recommends openjdk-11-jre-headless -yqq
USER airflow
RUN pip3 install pyspark==2.4.5 apache-airflow-providers-apache-spark==1.0.3