#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID to the dedicated RetryPermit project.}"
: "${CONFIRM_LIVE_TESTS_PASSED:?Set CONFIRM_LIVE_TESTS_PASSED=yes only after both live tests pass.}"

if [[ "${GCP_PROJECT_ID}" != *retrypermit* ]]; then
  echo "Refusing update: the dedicated project id must contain 'retrypermit'." >&2
  exit 2
fi

if [[ "${CONFIRM_LIVE_TESTS_PASSED}" != "yes" ]]; then
  echo "Refusing update without CONFIRM_LIVE_TESTS_PASSED=yes." >&2
  exit 2
fi

SERVICE="${SERVICE:-retrypermit}"
GCP_REGION="${GCP_REGION:-us-central1}"

gcloud run services update "${SERVICE}" \
  --region "${GCP_REGION}" \
  --project "${GCP_PROJECT_ID}" \
  --update-env-vars CLOUD_DEPLOYMENT_VERIFIED=true \
  --quiet >/dev/null

SERVICE_URL="$(
  gcloud run services describe "${SERVICE}" \
    --region "${GCP_REGION}" \
    --project "${GCP_PROJECT_ID}" \
    --format='value(status.url)' --quiet
)"

HEALTH="$(curl --fail --silent --show-error "${SERVICE_URL}/health")"
if [[ "${HEALTH}" != *'"cloud_deployment_verified":true'* ]]; then
  echo "The service did not report the verified flag." >&2
  exit 1
fi

echo "Cloud verification flag is live at ${SERVICE_URL}/health"
