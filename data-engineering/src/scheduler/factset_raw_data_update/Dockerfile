FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY src/scheduler/factset_raw_data_update/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/pipeline /app/pipeline
COPY config /config
COPY src/scheduler/factset_raw_data_update/factset_raw_data_update.py /app/

ENV PYTHONPATH=/app/
# Production containers write to GCS
ENV WRITE_MODE=GCS

CMD ["python", "factset_raw_data_update.py"]