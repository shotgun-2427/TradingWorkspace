FROM python:3.11-slim

WORKDIR /app

# Install system dependencies required for building packages
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Rust and Cargo
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"


# Install Python dependencies
COPY src/production/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/production/ /app/production
COPY src/config /app/config
COPY src/pipeline /app/pipeline

ENV PYTHONPATH=/app/

CMD ["python", "production/main.py"]