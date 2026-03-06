FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY src/scheduler/factset_bofa_bond_indices/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/pipeline /app/pipeline
COPY config /config
COPY src/scheduler/factset_bofa_bond_indices/factset_bofa_bond_indices.py /app/

ENV PYTHONPATH=/app/
# Production containers always write to BigQuery
ENV WRITE_MODE=BIGQUERY

CMD ["python", "factset_bofa_bond_indices.py"]
