# RetryPermit decisions

Decision records are append-only in spirit: update a decision when evidence changes, but do not erase the reason an earlier choice was made.

## ADR-001 — Rename the product to RetryPermit

**Status:** Accepted  
**Date:** 2026-08-26

The product is named **RetryPermit**, with the descriptor “policy-governed recovery for failed events.” “Backout” suggests rollback, while this system evaluates whether a failed event is permitted to be repaired and replayed. New code, UI, package metadata, and documentation use RetryPermit. Historical prompt filenames may retain the old word until they are deliberately migrated; their presence does not change the product identity.

This name is a working product decision, not legal clearance. Trademark, company-name, package-registry, app-store, and domain checks remain necessary before commercial release.

## ADR-002 — Baseline contained no executable test suite

**Status:** Recorded  
**Date:** 2026-08-26

At the implementation baseline, the workspace contained phase prompts and a commercial brief but no application source tree or test files. There was therefore no pre-existing executable test suite to preserve or report as passing. Phase 1 must establish the first test baseline, and future handoffs must report the exact command, pass count, skip count, and failures from an actual run.

## ADR-003 — One Cloud Run service for the sprint vertical slice

**Status:** Accepted for Phase 1  
**Date:** 2026-08-26

The React/Vite interface will be compiled and served by FastAPI. The API, Pub/Sub ingress, Cloud Tasks worker endpoint, and simulated downstream will live in one Cloud Run service for Phase 1.

Separate API and worker services would be preferable at production scale. One Cloud Run service gives one deploy, one cold-start domain, and one log stream for a short sprint, while Pub/Sub and Cloud Tasks still provide real transport and processing boundaries. Handlers remain separated behind interfaces so they can be split later without replacing the domain model.

## ADR-004 — Durable inbox plus deterministic Cloud Tasks acknowledgement

**Status:** Accepted  
**Date:** 2026-08-26

Cloud-mode Pub/Sub push does not run untracked background work and does not acknowledge merely because an envelope was observed.

The ingress handler verifies the OIDC identity and envelope, transactionally records a durable inbox/delivery item, and creates a Cloud Task with a deterministic name. It returns `2xx` only when the inbox record is durable and task scheduling is known to have succeeded. If task creation fails, it returns non-`2xx`; Pub/Sub redelivery reuses the inbox item and retries task creation. `ALREADY_EXISTS` for the same deterministic task is treated as scheduled. A recovery sweep also repairs durable inbox records that were saved but not scheduled or completed.

Cloud Tasks invokes a private worker route with an OIDC identity. Business orchestration remains idempotent, so duplicate Pub/Sub deliveries or task attempts cannot create a second downstream effect.

Local mode processes inline and uses the in-memory repository. It must be labeled as a convenience path without durable-delivery or cloud-deployment proof.

## ADR-005 — Cloud Tasks for per-message rechecks; Cloud Scheduler for recovery

**Status:** Accepted architecture; only the Phase 1 recovery subset is implemented in Phase 1  
**Date:** 2026-08-26

Future deferred-message rechecks use individually scheduled Cloud Tasks. This preserves a durable per-message schedule and avoids relying on a continuously running Cloud Run instance.

A Cloud Scheduler job calls a protected recovery endpoint on a bounded cadence. The sweep searches for:

- expired replay leases;
- durable inbox items that were recorded but not scheduled or not completed; and
- retryable or deferred messages whose `next_attempt_at` has arrived.

The sweep schedules deterministic tasks rather than performing replay effects itself. This keeps recovery idempotent and safe under overlapping scheduler invocations. Phase 1 implements only recovery needed by its durable inbox and replay path; full deferral/recheck behavior remains later-phase scope.

## ADR-006 — Google ADK 2.x with global Gemini model location

**Status:** Accepted, runtime verification pending  
**Date:** 2026-08-26

Use Google Agent Development Kit 2.x and pin the exact resolved 2.x version in the dependency lock. The single model-backed operation is the structured triage proposal; deterministic application code owns validation and effects.

Configure the model independently from infrastructure location:

- `GEMINI_MODEL=gemini-3.7-flash`
- `GOOGLE_CLOUD_LOCATION=global`
- Cloud Run, Cloud Tasks, Artifact Registry, and the new Firestore database: `us-central1`

The cloud preflight did not prove model availability because Vertex AI was disabled in the inspected billed project. Do not claim the model works until a real authenticated structured-output call succeeds in the dedicated project. Verify the exact ADK 2.x configuration schema before adding any thinking or reasoning parameter. If ADK blocks the vertical slice beyond the agreed time box, record the exact failure and fall back to the Google Gen AI SDK without changing the deterministic authorization boundary.

## ADR-007 — PDF policy extraction moves to Phase 2

**Status:** Accepted cut  
**Date:** 2026-08-26

Phase 1 uses a checked-in, validated, preapproved JSON policy fixture. PDF extraction is cut from Phase 1 so it cannot delay the Pub/Sub, Cloud Tasks, Firestore, replay-ledger, idempotent-downstream, and deployment gates.

Phase 2 may add PDF extraction behind the same policy-provider interface. Extracted policies always land as `PENDING_APPROVAL`; extraction alone never activates or authorizes a policy.

## ADR-008 — Reset rotates runs; cleanup is explicit and guarded

**Status:** Accepted  
**Date:** 2026-08-26

Reset creates a fresh run instead of erasing the previous run. The previous run is marked inactive and receives a 30-day expiration timestamp so its messages, transitions, receipts, deliveries, ledger links, and effect references remain available for audit and crash reconciliation.

Firestore does not recursively delete subcollections when a parent document is deleted. The Phase 1 cleanup utility therefore walks expired synthetic runs and their known child collections explicitly. It is dry-run by default, refuses project IDs that do not contain `retrypermit`, and requires the exact `--execute --confirm=delete-expired-synthetic-runs` acknowledgement before deletion. It must be run only against the dedicated project after reviewing its dry-run output.

## ADR-009 — Reproducible build and actual Tailwind compilation

**Status:** Accepted  
**Date:** 2026-08-26

The Python container installs the exact resolved `requirements.lock`, including `google-adk==2.8.0`, before installing the local application without re-resolving dependencies. The frontend uses the checked-in npm lock and Tailwind 3 through PostCSS; Tailwind is exercised by the compiled application rather than listed as an unused dependency. PostCSS is pinned to `8.5.26`, and the final high-severity npm audit reports zero vulnerabilities.

## Truthful claim checklist

The following statements remain false until the corresponding evidence exists:

- [x] “Tests pass” — `.venv/bin/pytest -q` completed with `27 passed, 2 skipped, 1 warning in 0.57s`; the skips are credentialed live-integration tests.
- [x] “Gemini is integrated” — the live structured-output test passed with `gemini-3.7-flash` in the dedicated project.
- [x] “ADK is integrated” — the production path and separate credentialed test exercised the pinned ADK 2.x package.
- [x] “Deployed to Cloud Run” — the public service is healthy in `us-central1`, with min `0` and max `1`.
- [x] “Authenticated Pub/Sub push works” — six genuine OIDC-authenticated deliveries reached the deployed ingress.
- [x] “Cloud Tasks is durable” — deterministic tasks called the worker with OIDC, and the recovery sweep reconciled an interrupted run.
- [x] “Firestore persistence works” — the acceptance run persisted messages, transitions, receipts, deliveries, replay ledgers, and downstream effects.
- [x] “Effectively-once business effects” — verified locally and in Firestore by six unique effects for six idempotency keys, including recovery of the interrupted first cloud run.
- [x] “Budget safeguards exist” — a project-scoped $10 monthly budget has 50%, 80%, and 100% alerts; it is an alert, not a hard cap.
- [x] “Evidence captured” — the verified console and application screenshots are documented in `docs/cloud-setup-learning.md`.

Always say “at-least-once delivery with effectively-once downstream business effects.” Never shorten this into an exactly-once-delivery claim. Always disclose synthetic data, the simulated downstream, and whether the current run is local or Google Cloud.
