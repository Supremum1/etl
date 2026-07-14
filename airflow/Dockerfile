FROM apache/airflow:2.10.3-python3.12

USER root
COPY python-cli/requirements.txt /tmp/requirements.txt
RUN chown airflow:0 /tmp/requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY --chown=airflow:0 python-cli /benchmark/python-cli
RUN pip install --no-cache-dir -e /benchmark/python-cli
