#!/usr/bin/env bash
set -euo pipefail

IMAGE="gcr.io/wsb-hc-qasap-ae2e/hawk-backfill:latest"
JOB="hawk-backfill"
REGION="us-central1"
SERVICE_ACCOUNT="prod-job-runner@wsb-hc-qasap-ae2e.iam.gserviceaccount.com"

if [[ -z "${FACTSET_API_KEY:-}" || -z "${FACTSET_USERNAME:-}" ]]; then
  echo "FACTSET_API_KEY and FACTSET_USERNAME must be set."
  exit 1
fi

docker buildx build \
  --platform linux/amd64 \
  -t "${IMAGE}" \
  -f src/api/backfill_job/Dockerfile \
  --push \
  .

gcloud run jobs update "${JOB}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --service-account "${SERVICE_ACCOUNT}" \
  --task-timeout=3600 \
  --max-retries=1 \
  --set-env-vars="FACTSET_API_KEY=${FACTSET_API_KEY},FACTSET_USERNAME=${FACTSET_USERNAME}"

echo "Updated Cloud Run job ${JOB} in ${REGION}."

