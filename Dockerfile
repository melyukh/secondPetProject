FROM apache/airflow:2.9.0

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    curl -sS https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -o /opt/airflow/postgresql-42.7.3.jar

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
RUN pip install \
    pyspark==3.5.1 \
    py4j==0.10.9.7 \
    pandas \
    python-dotenv \
    apache-airflow-providers-postgres \
    requests \
    matplotlib \
    seaborn