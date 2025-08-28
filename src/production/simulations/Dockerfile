FROM python:3.11-slim

WORKDIR /app

RUN echo "Installing system dependencies and Python packages..."
RUN ls -l
RUN echo "Installing system dependencies and Python packages..."

# Install system dependencies required for building packages
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Rust and Cargo
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install Python dependencies
COPY src/production/simulations/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/production/simulations /app/production/simulations
COPY src/common /app/common
COPY src/trading_engine /app/trading_engine

ENV PYTHONPATH=/app/

CMD ["python", "production/simulations/main.py"]