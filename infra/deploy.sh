#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID to a dedicated, billed project.}"
: "${DEMO_ADMIN_TOKEN:?Set DEMO_ADMIN_TOKEN to a long random value.}"
: "${CONFIRM_RETRYPERMIT_PROJECT:?Set CONFIRM_RETRYPERMIT_PROJECT=yes after verifying the target project is dedicated.}"
: "${CONFIRM_BUDGET_SAFEGUARDS:?Set CONFIRM_BUDGET_SAFEGUARDS=yes after verifying a billing budget and alerts.}"

if [[ "${CONFIRM_RETRYPERMIT_PROJECT}" != "yes" ]]; then
  echo "Refusing deployment without CONFIRM_RETRYPERMIT_PROJECT=yes." >&2
  exit 2
fi

if [[ "${CONFIRM_BUDGET_SAFEGUARDS}" != "yes" ]]; then
  echo "Refusing deployment without verified budget safeguards." >&2
  exit 2
fi

if [[ "${GCP_PROJECT_ID}" != *retrypermit* ]]; then
  echo "Refusing deployment: the dedicated project id must contain 'retrypermit'." >&2
  exit 2
fi

SERVICE="${SERVICE:-retrypermit}"
GCP_REGION="${GCP_REGION:-us-central1}"
GEMINI_LOCATION="${GEMINI_LOCATION:-global}"
FIRESTORE_LOCATION="${FIRESTORE_LOCATION:-us-central1}"
TASK_QUEUE="${TASK_QUEUE:-retrypermit-processing}"
TOPIC="${TOPIC:-orders.dlq}"
SUBSCRIPTION="${SUBSCRIPTION:-${TOPIC//./-}-push}"

RUNTIME_SA="retrypermit-runtime@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
PUSH_SA="retrypermit-pubsub@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
TASK_SA="retrypermit-tasks@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
RECOVERY_SA="retrypermit-recovery@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
PROJECT_NUMBER="$(
  gcloud projects describe "${GCP_PROJECT_ID}" \
    --format='value(projectNumber)' --quiet
)"
BUILD_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

gcloud services enable \
  run.googleapis.com pubsub.googleapis.com firestore.googleapis.com \
  cloudbuild.googleapis.com artifactregistry.googleapis.com \
  cloudtasks.googleapis.com aiplatform.googleapis.com \
  cloudscheduler.googleapis.com iamcredentials.googleapis.com \
  secretmanager.googleapis.com \
  --project "${GCP_PROJECT_ID}" --quiet

ensure_service_account() {
  local short_name="$1"
  local display_name="$2"
  if ! gcloud iam service-accounts describe \
      "${short_name}@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
      --project "${GCP_PROJECT_ID}" --quiet >/dev/null 2>&1; then
    gcloud iam service-accounts create "${short_name}" \
      --display-name "${display_name}" --project "${GCP_PROJECT_ID}" --quiet
  fi
}

ensure_service_account retrypermit-runtime "RetryPermit runtime"
ensure_service_account retrypermit-pubsub "RetryPermit Pub/Sub push"
ensure_service_account retrypermit-tasks "RetryPermit Cloud Tasks"
ensure_service_account retrypermit-recovery "RetryPermit recovery sweep"

grant_project_role() {
  local member="$1"
  local role="$2"
  gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
    --member "serviceAccount:${member}" --role "${role}" \
    --condition=None --quiet >/dev/null
}

grant_project_role "${RUNTIME_SA}" roles/datastore.user
grant_project_role "${RUNTIME_SA}" roles/pubsub.publisher
grant_project_role "${RUNTIME_SA}" roles/cloudtasks.enqueuer
grant_project_role "${RUNTIME_SA}" roles/aiplatform.user
grant_project_role "${RUNTIME_SA}" roles/logging.logWriter
grant_project_role "${BUILD_SA}" roles/run.builder
gcloud iam service-accounts add-iam-policy-binding "${TASK_SA}" \
  --project "${GCP_PROJECT_ID}" \
  --member "serviceAccount:${RUNTIME_SA}" \
  --role roles/iam.serviceAccountUser --quiet >/dev/null

ADMIN_SECRET="retrypermit-admin-token"
if ! gcloud secrets describe "${ADMIN_SECRET}" --project "${GCP_PROJECT_ID}" \
    --quiet >/dev/null 2>&1; then
  gcloud secrets create "${ADMIN_SECRET}" --replication-policy=automatic \
    --project "${GCP_PROJECT_ID}" --quiet
fi
gcloud secrets add-iam-policy-binding "${ADMIN_SECRET}" \
  --project "${GCP_PROJECT_ID}" \
  --member "serviceAccount:${RUNTIME_SA}" \
  --role roles/secretmanager.secretAccessor --quiet >/dev/null
printf '%s' "${DEMO_ADMIN_TOKEN}" | gcloud secrets versions add "${ADMIN_SECRET}" \
  --data-file=- --project "${GCP_PROJECT_ID}" --quiet >/dev/null

if ! gcloud firestore databases describe --database='(default)' \
    --project "${GCP_PROJECT_ID}" --quiet >/dev/null 2>&1; then
  gcloud firestore databases create --database='(default)' \
    --location "${FIRESTORE_LOCATION}" --type=firestore-native \
    --project "${GCP_PROJECT_ID}" --quiet
fi

if ! gcloud tasks queues describe "${TASK_QUEUE}" --location "${GCP_REGION}" \
    --project "${GCP_PROJECT_ID}" --quiet >/dev/null 2>&1; then
  gcloud tasks queues create "${TASK_QUEUE}" --location "${GCP_REGION}" \
    --max-concurrent-dispatches=10 --max-dispatches-per-second=10 \
    --project "${GCP_PROJECT_ID}" --quiet
fi

gcloud run deploy "${SERVICE}" --source . --region "${GCP_REGION}" \
  --project "${GCP_PROJECT_ID}" --service-account "${RUNTIME_SA}" \
  --allow-unauthenticated --min=0 --max=1 --concurrency=20 \
  --cpu=1 --memory=1Gi --timeout=300 \
  --set-env-vars="APP_ENV=cloud,DEMO_MODE=false,USE_IN_MEMORY_STORE=false,USE_FAKE_MODEL=false,CLOUD_DEPLOYMENT_VERIFIED=false,GOOGLE_CLOUD_PROJECT=${GCP_PROJECT_ID},GOOGLE_CLOUD_LOCATION=${GEMINI_LOCATION},GOOGLE_GENAI_USE_ENTERPRISE=TRUE,GEMINI_MODEL=gemini-3.7-flash,CLOUD_RUN_REGION=${GCP_REGION},PUBSUB_TOPIC=${TOPIC},PUBSUB_SUBSCRIPTION=projects/${GCP_PROJECT_ID}/subscriptions/${SUBSCRIPTION},CLOUD_TASKS_LOCATION=${GCP_REGION},CLOUD_TASKS_QUEUE=${TASK_QUEUE},SERVICE_BASE_URL=https://pending.invalid,OIDC_AUDIENCE=https://pending.invalid,PUBSUB_PUSH_SERVICE_ACCOUNT=${PUSH_SA},CLOUD_TASKS_SERVICE_ACCOUNT=${TASK_SA},RECOVERY_SERVICE_ACCOUNT=${RECOVERY_SA}" \
  --set-secrets="DEMO_ADMIN_TOKEN=${ADMIN_SECRET}:latest" \
  --quiet

SERVICE_URL="$(gcloud run services describe "${SERVICE}" --region "${GCP_REGION}" \
  --project "${GCP_PROJECT_ID}" --format='value(status.url)' --quiet)"

gcloud run services update "${SERVICE}" --region "${GCP_REGION}" \
  --project "${GCP_PROJECT_ID}" \
  --update-env-vars="SERVICE_BASE_URL=${SERVICE_URL},OIDC_AUDIENCE=${SERVICE_URL},PUBSUB_PUSH_SERVICE_ACCOUNT=${PUSH_SA},PUBSUB_SUBSCRIPTION=projects/${GCP_PROJECT_ID}/subscriptions/${SUBSCRIPTION},CLOUD_TASKS_SERVICE_ACCOUNT=${TASK_SA},RECOVERY_SERVICE_ACCOUNT=${RECOVERY_SA}" \
  --quiet

for service_account in "${PUSH_SA}" "${TASK_SA}" "${RECOVERY_SA}"; do
  gcloud run services add-iam-policy-binding "${SERVICE}" \
    --region "${GCP_REGION}" --project "${GCP_PROJECT_ID}" \
    --member "serviceAccount:${service_account}" --role roles/run.invoker --quiet >/dev/null
done

PUBSUB_AGENT="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"
gcloud iam service-accounts add-iam-policy-binding "${PUSH_SA}" \
  --project "${GCP_PROJECT_ID}" \
  --member "serviceAccount:${PUBSUB_AGENT}" \
  --role roles/iam.serviceAccountTokenCreator --quiet >/dev/null

if ! gcloud pubsub topics describe "${TOPIC}" --project "${GCP_PROJECT_ID}" --quiet >/dev/null 2>&1; then
  gcloud pubsub topics create "${TOPIC}" --project "${GCP_PROJECT_ID}" --quiet
fi

if gcloud pubsub subscriptions describe "${SUBSCRIPTION}" --project "${GCP_PROJECT_ID}" --quiet >/dev/null 2>&1; then
  gcloud pubsub subscriptions update "${SUBSCRIPTION}" \
    --push-endpoint="${SERVICE_URL}/pubsub/dlq" \
    --push-auth-service-account="${PUSH_SA}" \
    --push-auth-token-audience="${SERVICE_URL}" \
    --project "${GCP_PROJECT_ID}" --quiet
else
  gcloud pubsub subscriptions create "${SUBSCRIPTION}" \
    --topic "${TOPIC}" --push-endpoint="${SERVICE_URL}/pubsub/dlq" \
    --push-auth-service-account="${PUSH_SA}" \
    --push-auth-token-audience="${SERVICE_URL}" \
    --min-retry-delay=10s --max-retry-delay=60s \
    --project "${GCP_PROJECT_ID}" --quiet
fi

if gcloud scheduler jobs describe retrypermit-recovery --location "${GCP_REGION}" \
    --project "${GCP_PROJECT_ID}" --quiet >/dev/null 2>&1; then
  gcloud scheduler jobs update http retrypermit-recovery \
    --location "${GCP_REGION}" --schedule='*/5 * * * *' \
    --uri="${SERVICE_URL}/internal/recovery/sweep" --http-method=POST \
    --oidc-service-account-email="${RECOVERY_SA}" \
    --oidc-token-audience="${SERVICE_URL}" \
    --update-headers='Content-Type=application/json' --message-body='{"limit":100}' \
    --project "${GCP_PROJECT_ID}" --quiet
else
  gcloud scheduler jobs create http retrypermit-recovery \
    --location "${GCP_REGION}" --schedule='*/5 * * * *' \
    --uri="${SERVICE_URL}/internal/recovery/sweep" --http-method=POST \
    --oidc-service-account-email="${RECOVERY_SA}" \
    --oidc-token-audience="${SERVICE_URL}" \
    --headers='Content-Type=application/json' --message-body='{"limit":100}' \
    --project "${GCP_PROJECT_ID}" --quiet
fi

curl --fail --silent --show-error "${SERVICE_URL}/health"
echo
echo "Verified health URL: ${SERVICE_URL}/health"
echo "The end-to-end Pub/Sub and Gemini smoke tests must still be run before claiming deployment complete."
