FROM python:3.11-slim

WORKDIR /app

# Install the same dependencies the data-generator uses plus pyyaml for tests
RUN pip install --no-cache-dir \
    clickhouse-connect==0.7.19 \
    faker==28.4.1 \
    numpy==1.26.4 \
    "pandas>=2.0.0" \
    python-dotenv==1.0.1 \
    pyyaml
