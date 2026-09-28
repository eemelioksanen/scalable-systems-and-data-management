# Canonical environment for the oracle. Correctness evidence cites runs made here;
# oracle/run.sh reproduces the same output in a local venv for faster iteration.
FROM python:3.12-slim

RUN useradd --create-home --uid 1000 oracle
WORKDIR /oracle

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER oracle
ENV ORACLE_OUT=/out PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["python3"]
