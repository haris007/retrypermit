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

**Status:** Accepted and extended in Phase 2
**Date:** 2026-08-26

Deferred-message rechecks use individually scheduled Cloud Tasks. Phase 2 extends the durable worker path so a replay-stage retry immediately receives a deterministic future task at `next_attempt_at`, rather than waiting for a global sweep.

A Cloud Scheduler job calls a protected recovery endpoint on a bounded cadence. The sweep searches for:

- expired replay leases;
- durable inbox items that were recorded but not scheduled or not completed; and
- retryable or deferred messages whose `next_attempt_at` has arrived.

The sweep schedules deterministic tasks rather than performing replay effects itself. This keeps recovery idempotent and safe under overlapping scheduler invocations. Full Class B deferral/resume behavior remains Phase 3 scope; Phase 2 does not replace this decision with a sweep loop.

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

## ADR-010 — Ambiguous outcome injection belongs after the effect

**Status:** Accepted
**Date:** 2026-08-27

The per-run `inject_failure` control causes exactly one designated simulated downstream request to create its idempotent business effect first and then raise a deliberate lost-response/503 error. It does not inject a Pub/Sub or Cloud Tasks delivery fault. Retry uses the same payload-bound key, so the downstream returns the original reference and increments only the request count, not the unique effect count.

Replay delays use bounded exponential backoff with up to 25 percent jitter and the active policy's explicit retry budget. Model, downstream, and PDF extraction calls all have separate timeouts. Retry routing is always derived from `failed_stage`.

## ADR-011 — Extracted PDF policies have no authority until audited activation

**Status:** Accepted
**Date:** 2026-08-27

Gemini through Google ADK extracts an untrusted PDF into the strict `PolicyDefinition` schema. Deterministic code validates the tenant, schema, repairs, clauses, hashes, and application ceiling. The new version is stored as `PENDING_APPROVAL`; a separate protected approval creates a `policy_approval_events` record, and a protected activation creates a `policy_activation_events` record with actor, timestamp, previous version, and new version.

Startup preserves whichever version is currently active. It seeds and activates v1 only when the tenant has no active policy, preventing a Cloud Run cold start from undoing an approved v2 activation.

## ADR-012 — Live duplicate-redelivery control cut from Phase 2

**Status:** Accepted cut
**Date:** 2026-08-27

The optional UI control to repost an identical Pub/Sub envelope was cut to keep the phase focused on the mandatory ambiguous-outcome proof and policy lifecycle. Duplicate delivery remains covered by automated delivery/store tests. Demo material must state that limitation and must not imply spontaneous Pub/Sub redelivery.

## ADR-013 — Phase 3 outcomes remain task-driven and fail closed

**Status:** Accepted
**Date:** 2026-08-27

Class B uses the Phase 2 scheduling decision without introducing a production sweep loop. A transient inventory 503 moves `PLANNED → DEFERRED`, persists its reason, clause, attempt count, and `recheck_at`, and causes the durable inbox path to schedule one deterministic task for that message. At the deadline the worker moves `DEFERRED → RECHECKING`; an unavailable dependency returns to `DEFERRED`, while recovery moves through `PLANNED` and replays the unchanged, hash-validated payload. Cloud Scheduler only schedules recovery tasks for missed or stale work.

The local demo uses the recording scheduler as a deterministic task executor and exposes a 10-second interval override. Production reads the interval from the active policy (`transient_recheck_seconds`, currently 300). This local harness is not presented as durable Cloud Tasks evidence.

Class C never guesses a business value. Negative quantity and disallowed currency records include a proposed fix, withheld reason, severity, SLA, synthetic recipient, policy clause, and an escalation receipt, then terminate as `ESCALATED` with no downstream effect. Class D treats every payload string as inert data. Either the deterministic instruction detector or the model flag forces `QUARANTINED`, produces a refusal receipt, and permits no repaired payload or downstream effect. Model confidence remains evidence, never authority.

## ADR-014 — Phase 4 operations console, bounded concurrency, and feature freeze

**Status:** Accepted and frozen
**Date:** 2026-08-27

The final hackathon console uses one operations screen. It keeps the run outcome counters and controls above a two-column investigation workspace: all 12 color-coded messages remain in the left rail, while the selected message's decision, payload diff, repairs, transition timeline, receipt phases, and effectively-once proof form one continuous right-hand story. Coral escalation/failure and purple quarantine are intentionally separate. Synthetic-data, simulated-downstream, demo-mode, and model-advisory disclosures remain visible.

The browser polls the read API every 1.8 seconds. Server-sent events were not added for the sprint because polling tolerates Cloud Run instance/request boundaries, recovers naturally after transient disconnects, and requires no additional connection lifecycle. Message list data is fetched in one request; only the currently selected message receives detail requests.

Policy lookup is cached by tenant and active version and invalidated immediately after activation. Initial triage runs concurrently behind a configurable semaphore, defaulting to four workers. This bounds future ADK/Gemini pressure without changing the production adapter. Reset remains an audited namespace rotation: the active screen becomes a fresh 12-message run while prior runs remain available for evidence and expiration-based cleanup.

The local feature-freeze gate ran three consecutive real-cadence scenarios without source changes between them:

- clean v1: 10.020 seconds;
- injected lost-response v1: 10.022 seconds; and
- selected v2: 10.021 seconds.

Every run completed under 90 seconds with the identical final outcome: 9 replayed, 3 transient messages recovered after deferral, 2 escalated, 1 quarantined, and DLQ depth 3. All runs used the in-memory store and deterministic fixture and made zero Google Cloud calls. The reusable gate is `scripts/phase4_freeze_gate.py`.

Feature freeze began on 2026-08-27 after this gate. Until Phase 5 starts, only documentation, tests, and bug fixes may change. Captured evidence is stored in:

- `docs/media/phase4-operations-overview.jpg`;
- `docs/media/phase4-effectively-once-evidence.jpg`; and
- `docs/media/phase4-local-lost-response-demo.mp4`.

## ADR-015 — Phase 5 is documentation/tests only; final cloud evidence stays gated

**Status:** Accepted
**Date:** 2026-08-27

Phase 5 adds no product functionality. It completes the test-to-requirement matrix, judge-facing README, architecture/state/security/runbook documentation, Devpost draft, and video rehearsal script using the deterministic fake model and in-memory store. The preserved ADK/Gemini, Firestore, Pub/Sub, Cloud Tasks, Scheduler, IAM, Secret Manager, and Cloud Run paths remain unchanged.

The deployed acceptance test is updated to the frozen 12-message contract: 9 replayed, 2 escalated, 1 quarantined, and 3 messages left in DLQ accounting. Its default deadline is 420 seconds because cloud mode correctly honors the approved policy's 300-second transient recheck rather than the local 10-second override. This test is not executed during local Phase 5 and remains an explicit skip until the owner authorizes live credentials and billable calls.

The four-minute video has an unresolved timing constraint: the real 300-second Class B recheck cannot both start and finish inside the evaluated four minutes. The owner must either approve a separate, auditable synthetic demo policy with a shorter recheck (using the existing policy mechanism, not a code bypass) or show an unedited initial live run followed by timestamped evidence from a previously completed cloud run. No production policy is changed during the local Phase 5 pass.

The current 12-message fixture does not contain the historical $750 Phase 2 order, so v1 and v2 have the same final outcome in the frozen console. Policy-caused behavior change remains truthfully demonstrated by the dedicated Phase 2 test and evidence; the final video must not imply the current 12-message run changes under v2.

The completed local Phase 5 verification reported `42 passed, 2 skipped, 1 upstream deprecation warning`; Ruff check/format, the frontend production build, Markdown target check, secret-pattern scan, and three-scenario freeze rehearsal all passed. The skipped deployed vertical-slice and real-Gemini tests are not counted as passing and remain the explicit final cloud gate.

## Truthful claim checklist

The following statements remain false until the corresponding evidence exists:

- [x] “Tests pass” — the Phase 1–4 local suite completed with `42 passed, 2 skipped`; the skips are credentialed live-integration tests.
- [x] “Gemini is integrated” — the live structured-output test passed with `gemini-3.7-flash` in the dedicated project.
- [x] “ADK is integrated” — the production path and separate credentialed test exercised the pinned ADK 2.x package.
- [x] “Deployed to Cloud Run” — the public service is healthy in `us-central1`, with min `0` and max `1`.
- [x] “Authenticated Pub/Sub push works” — six genuine OIDC-authenticated deliveries reached the deployed ingress.
- [x] “Cloud Tasks is durable” — deterministic tasks called the worker with OIDC, and the recovery sweep reconciled an interrupted run.
- [x] “Firestore persistence works” — the acceptance run persisted messages, transitions, receipts, deliveries, replay ledgers, and downstream effects.
- [x] “Effectively-once business effects” — verified locally and in Firestore by six unique effects for six idempotency keys, including recovery of the interrupted first cloud run.
- [x] “Budget safeguards exist” — a project-scoped $10 monthly budget has 50%, 80%, and 100% alerts; it is an alert, not a hard cap.
- [x] “Evidence captured” — the verified console and application screenshots are documented in `docs/cloud-setup-learning.md`.
- [x] “Ambiguous outcome is effectively once” — the local live proof queried two replay attempts, two downstream requests, and one unique effect with the original reference.
- [x] “Policy v2 changes behavior without code” — the local end-to-end test escalates the $750 order under v1 and replays all six after audited v2 activation.
- [x] “PDF policy extraction is live” — a credentialed ADK/Gemini PDF call produced strict runbook v2, and the deployed approval/activation acceptance path passed.
- [x] “Phase 3 Class B/C/D behavior works locally” — deterministic fixture tests prove scheduled deferral/recheck/replay, withheld escalation, quarantine/refusal, and zero effects for Classes C and D.
- [ ] “Phase 3 works in the deployed cloud path” — intentionally not reverified because this phase was authorized for local fake-model execution only.
- [x] “Phase 4 is locally frozen” — three real-cadence runs completed in about 10.02 seconds each with identical outcomes, including injected failure and active v2.
- [ ] “Phase 4 works in the deployed cloud path” — intentionally not verified because no final cloud verification run was authorized.

Always say “at-least-once delivery with effectively-once downstream business effects.” Never shorten this into an exactly-once-delivery claim. Always disclose synthetic data, the simulated downstream, and whether the current run is local or Google Cloud.
