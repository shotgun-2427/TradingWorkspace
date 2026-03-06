FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY src/scheduler/raw_supplemental_data_update/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/pipeline /app/pipeline
COPY config /config
COPY src/scheduler/raw_supplemental_data_update/raw_supplemental_data_update.py /app/

ENV PYTHONPATH=/app/
# Production containers write to GCS
ENV WRITE_MODE=GCS

CMD ["python", "raw_supplemental_data_update.py"]