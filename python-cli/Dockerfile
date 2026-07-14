FROM python:3.12-slim

WORKDIR /benchmark

COPY python-cli/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY python-cli /benchmark/python-cli
RUN pip install --no-cache-dir -e /benchmark/python-cli

ENTRYPOINT ["python", "-m", "etlbench"]
