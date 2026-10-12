FROM python:3.12-slim

RUN useradd --create-home --uid 1000 check
WORKDIR /correctness

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER check
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/correctness:/oracle
ENTRYPOINT ["python3"]
