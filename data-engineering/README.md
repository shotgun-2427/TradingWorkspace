# **Data Engineering**

This repository is responsible for the **storage solutions and processes** that handle both vendor-supplied data and internally generated data.

---
## **Prerequisites**
```bash
export FACTSET_API_KEY=<your_factset_api_key>
export FACTSET_USERNAME=<your_factset_username>
export GOOGLE_APPLICATION_CREDENTIALS=</path/to/your/service_account.json>

# Application Performance Monitoring (OPTIONAL):
export NEWRELIC_API_KEY=<your_new_relic_api_key>
```
 - Python 3.10 or higher
---

## **Repository Access**

To clone the repository and start contributing, run:

```bash
git clone https://github.com/Hawk-Center/data-engineering.git
cd data-engineering
```

---

## **Manual Execution**

Before running the pipeline manually, ensure the necessary environment variables are set:

### **1. Navigate to the Repository Root**
Ensure you are in the root directory of the **data-engineering** repository:

```bash
cd /path/to/data-engineering
```

### **2. Set `PYTHONPATH`**
Set the `PYTHONPATH` to the root directory of the repository to ensure proper module resolution:
```bash
export PYTHONPATH=$(pwd)
```

### **3. Set `GOOGLE_APPLICATION_CREDENTIALS`**
Ensure that the Google Cloud service account credentials are set:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```

### **4. Execute the Pipeline**
Run the following command, replacing values as needed:
```bash
python pipeline/fields_pipeline/main.py -e <ENVIRONMENT> -o <OPERATION> -fn <FIELD_NAME> -t <FIELD_TYPE>
```

#### **Example Execution:**
```bash
python pipeline/fields_pipeline/main.py -e DEVELOPMENT -o ADD -fn TESTING_NEW_CLICK -t INTEGER
```

#### **Arguments:**
- `-e, --env` → The environment to run in (e.g., `DEVELOPMENT`, `PRODUCTION`).
- `-o, --operation` → The operation to perform (e.g., `ADD`, `UPDATE`, `DELETE`).
- `-fn, --field_name` → The name of the field to process.
- `-t, --field_type` → The type of the field (e.g., `INTEGER`, `DOUBLE`, `CHAR`).

### **4. Troubleshooting**
- If you encounter `ModuleNotFoundError: No module named 'pipeline'`, verify that `PYTHONPATH` is set correctly.
- Ensure the `GOOGLE_APPLICATION_CREDENTIALS` file exists and is accessible.
- If using a virtual environment, activate it before execution:
  ```bash
  source venv/bin/activate
  ```

---

## **Testing**

### **Pre-requisites**

1. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of your Google Cloud service account JSON key file:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
   ```

   - This step is mandatory for tests interacting with Google Cloud services.
   - Ensure the JSON key file has appropriate permissions and access to the necessary Google Cloud resources.

2. If you are using a virtual environment and executing test cases via an external terminal, make sure you are currently in your virtual environment
   ```bash
   # Check if the python path points to your virtual environment
   pip -V
   
   # If not, activate your virtual environment
   source venv/bin/activate
   ```

### **Running Test Cases**


Navigate to the root directory of the project and use the following commands:

1. **Run All Test Cases**
   To discover and run all test cases:
   ```bash
   python -m unittest discover
   ```

   - By default, this searches for test files matching the pattern `test*.py` in the current directory and subdirectories.

2. **Run Specific Test Cases**
   To execute specific test cases, provide the path to the test file:
   ```bash
   python -m unittest <path/to/test-case-file>
   ```

   **Example:**
   ```bash
   python -m unittest test/pipeline/fields_pipeline/test_fields_pipeline.py
   ```

### **Additional Suggestions**
- **Verbose Output**: For more detailed output, add the `-v` flag:
  ```bash
  python -m unittest discover -v
  ```

### **Troubleshooting**
- Ensure the environment variable `GOOGLE_APPLICATION_CREDENTIALS` is set correctly and points to a valid file.
- Check for any missing dependencies by running:
  ```bash
  pip install -r config/requirements.txt
  ```
- If you encounter permission issues with the JSON key file, verify its access levels.

---

## **Contributing**

We welcome contributions! Please read our **[CONTRIBUTING.md](.github/CONTRIBUTING.md)** for guidelines on how to contribute.

---

### **Setup**

This project uses **Poetry** for dependency management. To set up your environment follow these **[POETRY.md](.github/POETRY.md)** setup instructions.

---

### **Documentation**

To view the docs, run `mkdocs serve` and navigate to the provided localhost URL.

# FactSet Snapshot Service

This service pulls data from FactSet and loads it into Google BigQuery.

## Prerequisites

- Docker installed on your system
- Google Cloud service account credentials saved as `service_account.json` in the project root
- FactSet API credentials

## Configuration

Create an `.env` file in the project root with the following variables:

```
FACTSET_USERNAME=your_factset_username
FACTSET_API_KEY=your_factset_api_key
```

## Running with Docker

### Building the Docker image

```bash
docker build -t factset-snapshot-local .
```

### Running with Docker Compose

```bash
docker-compose up
```

### Running with Docker directly

```bash
docker run --rm \
  -e FACTSET_USERNAME=$(grep FACTSET_USERNAME .env | cut -d= -f2) \
  -e FACTSET_API_KEY=$(grep FACTSET_API_KEY .env | cut -d= -f2) \
  -e ENVIRONMENT=DEVELOPMENT \
  -v $(pwd)/service_account.json:/app/service_account.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/service_account.json \
  factset-snapshot-local
```

## Development

The service authenticates with Google Cloud using the service account specified in `service_account.json`. This file is mounted into the container at runtime.

The BigQuery client in `pipeline/common/bigquery_client.py` handles authentication and data loading operations.

