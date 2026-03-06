# Schedulers

This directory contains the job definitions for scheduled data pipelines. Each job is now configured as a Cloud Run Job
using the deployment scripts in the `/deployment` directory.

## Jobs

- **factset_snapshot**: Daily snapshot of FactSet equity data (runs at 3:30 PM EST / 19:30 UTC)
- **factset_eod**: End-of-day FactSet data pipeline for futures and equities (runs at 6:00 PM EST / 23:00 UTC)
- **factset_eod_csv_update**: Updates CSV files in GCS with FactSet data (runs at 6:30 PM EST / 23:30 UTC)

## Deployment

See the `.github/workflows/deploy.yml` for the deployment process. The deployment is triggered by changes to the `main`
branch and includes the following steps:

1. **Checkout Code**: Clones the repository.
2. **Set Up Python**: Configures the Python environment.
3. **Install Dependencies**: Installs required Python packages.
4. **Authenticate with GCP**: Authenticates with Google Cloud Platform using the service account.
5. **Configure Docker**: Sets up Docker to use the Google Cloud service account for authentication.
6. **Build Docker Image**: Builds the Docker image for the Cloud Run Job.
7. **Deploy Jobs**: Deploys the Cloud Run Jobs defined in this directory.
8. **Setup Cloud Scheduler**: Configures Cloud Scheduler to trigger the jobs at specified intervals.
