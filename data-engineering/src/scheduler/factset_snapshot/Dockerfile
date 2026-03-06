FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY src/scheduler/factset_snapshot/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/pipeline /app/pipeline
COPY config /config
COPY src/scheduler/factset_snapshot/factset_snapshot.py /app/

ENV PYTHONPATH=/app/
# Production containers always write to BigQuery
ENV WRITE_MODE=BIGQUERY

CMD ["python", "factset_snapshot.py"] 