FROM apache/airflow:2.2.3
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/airflow
COPY requirements.txt .

USER airflow
RUN pip install -r requirements.txt
COPY configs.py .