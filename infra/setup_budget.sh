#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID to the dedicated RetryPermit project.}"

BUDGET_DISPLAY_NAME="${BUDGET_DISPLAY_NAME:-RetryPermit Hackathon - 10 USD Guardrail}"
BUDGET_AMOUNT="${BUDGET_AMOUNT:-10USD}"

BILLING_ACCOUNT_NAME="$(
  gcloud billing projects describe "${GCP_PROJECT_ID}" \
    --format='value(billingAccountName)' --quiet
)"

if [[ -z "${BILLING_ACCOUNT_NAME}" ]]; then
  echo "The project is not linked to a billing account." >&2
  exit 2
fi

BILLING_ACCOUNT_ID="${BILLING_ACCOUNT_NAME#billingAccounts/}"

gcloud services enable billingbudgets.googleapis.com \
  --project "${GCP_PROJECT_ID}" --quiet

if gcloud billing budgets list \
    --billing-account "${BILLING_ACCOUNT_ID}" \
    --billing-project "${GCP_PROJECT_ID}" \
    --format='value(displayName)' --quiet | grep -Fxq "${BUDGET_DISPLAY_NAME}"; then
  echo "Budget already exists: ${BUDGET_DISPLAY_NAME}"
  exit 0
fi

gcloud billing budgets create \
  --billing-account "${BILLING_ACCOUNT_ID}" \
  --billing-project "${GCP_PROJECT_ID}" \
  --display-name "${BUDGET_DISPLAY_NAME}" \
  --budget-amount "${BUDGET_AMOUNT}" \
  --calendar-period month \
  --filter-projects "projects/${GCP_PROJECT_ID}" \
  --threshold-rule percent=0.50,basis=current-spend \
  --threshold-rule percent=0.80,basis=current-spend \
  --threshold-rule percent=1.00,basis=current-spend \
  --ownership-scope all-users \
  --quiet >/dev/null

echo "Created ${BUDGET_AMOUNT} monthly budget with 50%, 80%, and 100% alerts."
