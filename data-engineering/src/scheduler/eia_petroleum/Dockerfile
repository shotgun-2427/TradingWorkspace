FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY src/scheduler/eia_petroleum/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/pipeline /app/pipeline
COPY config /config
COPY src/scheduler/eia_petroleum/eia_petroleum.py /app/

ENV PYTHONPATH=/app/
# Production containers always write to BigQuery
ENV WRITE_MODE=BIGQUERY

CMD ["python", "eia_petroleum.py"]
