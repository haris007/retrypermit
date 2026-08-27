#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID to a dedicated RetryPermit project.}"
GCP_REGION="${GCP_REGION:-us-central1}"
GEMINI_LOCATION="${GEMINI_LOCATION:-global}"

echo "Account: $(gcloud auth list --filter=status:ACTIVE --format='value(account)' --quiet)"
echo "Project: ${GCP_PROJECT_ID}"
echo "Cloud Run/Tasks region: ${GCP_REGION}"
echo "Gemini location: ${GEMINI_LOCATION}"

gcloud projects describe "${GCP_PROJECT_ID}" \
  --format='table(projectId,name,lifecycleState)' --quiet
gcloud billing projects describe "${GCP_PROJECT_ID}" \
  --format='table(projectId,billingEnabled,billingAccountName)' --quiet

gcloud services list --project "${GCP_PROJECT_ID}" --enabled \
  --filter='config.name:(run.googleapis.com OR pubsub.googleapis.com OR firestore.googleapis.com OR cloudbuild.googleapis.com OR artifactregistry.googleapis.com OR cloudtasks.googleapis.com OR aiplatform.googleapis.com OR cloudscheduler.googleapis.com)' \
  --format='table(config.name)' --quiet

gcloud projects get-iam-policy "${GCP_PROJECT_ID}" \
  --flatten='bindings[].members' \
  --filter="bindings.members:user:$(gcloud auth list --filter=status:ACTIVE --format='value(account)' --quiet)" \
  --format='table(bindings.role)' --quiet

echo "Preflight is read-only. Verify a budget and gemini-3.7-flash access before deploy."

