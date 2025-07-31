FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY src/production/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/common /app/common
COPY src/production /app/production
COPY src/trading_engine /app/trading_engine

ENV PYTHONPATH=/app/

CMD ["python", "production/main.py"]