# Canonical environment for the dataset profile. The report cites results produced here.
# analysis/run.sh reproduces the same output in a local venv for faster iteration.
FROM python:3.12-slim

RUN useradd --create-home --uid 1000 analyst
WORKDIR /src

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER analyst
ENTRYPOINT ["python3"]
