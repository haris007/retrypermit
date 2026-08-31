# Final end-to-end verification - August 31, 2026

Engineering acceptance is complete for the hackathon's synthetic vertical slice. Submission itself is not complete: the final public video, owner declarations and Devpost submission remain.

## Deployed result

- [Public application](https://retrypermit-vm5aevgdva-uc.a.run.app)
- [Public repository](https://github.com/haris007/retrypermit)
- Tested application commit: `561b5136ab7946ae1a36fc6fe96498ed53b645e8`.
- Live test revision: `retrypermit-00020-gvk`.
- Current revision: `retrypermit-00021-szl`, same image, only the verified flag changed.
- Image: `sha256:9a9d60765a7a6edbd70b73e9ca1bcb8ee8a3ad2c87793e7bf0596f2f5b7230fa`.
- Final health: Google Cloud, Firestore, Google ADK / `gemini-3.7-flash`, `cloud_deployment_verified=true`.
- Existing administrator secret reference preserved; no secret version created or rotated. Cloud Run minimum remains zero and service maximum remains one.
- Every order, customer, policy, recipient and downstream effect is synthetic or simulated.

## Actual results

| Check | Result |
| --- | --- |
| Local suite with socket connections blocked | 66 passed, 2 expected cloud-only skips, 1 upstream Starlette/httpx warning |
| Ruff lint and format | Passed |
| TypeScript and Vite production build | Passed |
| Fake-only freeze rehearsals | Four passes: clean v1 10.020s; lost-response v1 10.023s; selected v2 10.020s; selected v3 10.026s |
| Local browser flow | Start with lost response, repair diff, Class B history, Class C withheld correction, Class D inert-text quarantine, live proof all checked |
| Real Google ADK/Gemini structured triage | Passed |
| Real Gemini PDF extraction | Exact reviewed v3 definition and source hash matched before approval; pending activation rejected |
| Live v1 lost-response workflow | 12 messages; 9 replayed, 2 escalated, 1 quarantined; 324.036s |
| Live v3 clean workflow | 12 messages; 9 replayed, 2 escalated, 1 quarantined; 332.047s |
| Independent Firestore evidence | 12 messages and exactly 9 unique effect records in each run |
| Final public UI | Google Cloud, active v3, 9/12 resolved, 2 escalated, 1 quarantined, DLQ depth 3 |
| Scoped credential-pattern scan | No Google API-key/private-key patterns found in reviewed source, tests, scripts, infra, docs, fixtures and README |

The three deferred orders are included in the nine replayed orders after recovery. The final current deferred count is zero; the console's Deferred card shows the three orders that went through deferral.

## Test correction and honest pass accounting

The first cloud command reported **1 passed, 1 failed in 695.60 seconds**. The real model test passed. Both fresh workflows, all receipt/effect checks, both in-flight policy-switch denials, repeat-Start idempotency, and policy-fingerprint preservation also passed. The final independent Firestore loop then incorrectly expected the first run's status to remain COMPLETE.

Reset intentionally archives the prior completed run as **INACTIVE**, while preserving `completed_at`, messages, receipts and effects. This behavior exists in both store implementations. The test was corrected to require the exact archived/current statuses, retained completion timestamps, and correct active flags. Fake-only tests were extended to verify archive preservation and all 66 tests passed again.

The same two completed runs were then revalidated through the corrected acceptance test's explicit **read-only continuation** mode: **1 passed in 12.64 seconds**. This made no new Gemini calls and no run/policy mutations. It rechecked every message's receipts, transitions, effect proof, v3 authority and independent Firestore records. This is a completed multi-step verification session, not a claim that the initial command passed or that a second fresh live run was performed.

## Run IDs and queryable proof

| Scenario | Run ID | Persistence status after verification |
| --- | --- | --- |
| v1 lost response | `run_66ec55cc81e44794b886a539ffb33013` | INACTIVE; completed at 16:51:18.223184 UTC |
| v3 clean | `run_6175618e7ec24e3f870d8ff654c759d2` | COMPLETE; completed at 16:56:55.322526 UTC |

[Lost-response proof](https://retrypermit-vm5aevgdva-uc.a.run.app/api/messages/synthetic-order-001/proof?run_id=run_66ec55cc81e44794b886a539ffb33013) shows 1 recorded Pub/Sub delivery, **2 execution attempts, 2 downstream requests, 1 unique effect**, CONFIRMED, original reference `rp_order_17e44e787026c421`.

[Clean v3 proof](https://retrypermit-vm5aevgdva-uc.a.run.app/api/messages/synthetic-order-001/proof?run_id=run_6175618e7ec24e3f870d8ff654c759d2) shows 1 attempt, 1 request, 1 effect and CONFIRMED.

These are effectively-once simulated business effects under at-least-once delivery, not exactly-once transport. Effect records are scoped to synthetic runs; intentionally resetting a demo is not a claim about global cross-run deduplication.

## What happened end to end

1. The protected Start API bound the run to its active approved policy and published 12 synthetic events to Pub/Sub `orders.dlq`.
2. Subscription `orders-dlq-push` delivered to `/pubsub/dlq` with the dedicated `retrypermit-pubsub` OIDC identity.
3. The ingress persisted a Firestore inbox and scheduled work in Cloud Tasks queue `retrypermit-processing` in `us-central1`.
4. Cloud Tasks called `/internal/tasks/process` with the dedicated `retrypermit-tasks` identity.
5. The ADK agent inside Cloud Run asked Gemini for a structured proposal. Deterministic policy and schema checks authorized or withheld actions.
6. Six Class A messages were repaired and replayed. Two Class C messages were escalated with human-confirmation proposals and no effects. Class D was quarantined with no effect.
7. Three Class B messages were actually deferred, with individual 300-second scheduled tasks. No manual recheck or local timing override was used.
8. The lost-response scenario committed one simulated effect, returned a deliberate 503, and retried the same key to obtain the original reference.
9. Receipts, state transitions, replay ledgers and independent Firestore effect records established the result.

For v3 message 7, DEFERRED was recorded at **16:51:43.942249 UTC**, RECHECKING at **16:56:44.202311 UTC**, and REPLAYED at **16:56:44.993142 UTC**. This proves the real five-minute interval, not just a class counter.

The Cloud Scheduler recovery job remains enabled on its five-minute schedule. The new revision recorded an automatic sweep HTTP 200 at 16:45:11 UTC; the acceptance test's admin-token-only and local-bypass attempts returned 401. Each running/deferred run rejected switching policy with 409.

## Immutable v3 policy

The historical cloud v2 lacked the Class B clause. Its definition and fingerprints were preserved. It was not deleted, rewritten or re-hashed.

The new [v3 PDF](../fixtures/dlq-runbook-v3.pdf) is generated from its [reviewed JSON](../fixtures/policies/runbook-v3.json), includes all four classes plus safe stop, and was visually inspected on all five pages. The fake provider selects it only by exact bundled bytes; that fixture test is not evidence of parsing an arbitrary PDF.

The real Gemini extraction was approved and activated through authenticated application routes under the owner's explicit authorization:

- Extracted: 16:45:52.940853 UTC; approved: 16:45:53.322570 UTC.
- Activated: 16:51:22.706164 UTC, after the first run completed.
- Source SHA-256: `cda80bbdf6f687cf9250a722b365b5bf88b12995c786693027af8650b7031812`.
- Policy SHA-256: `555a6be2b4b48c1abf22106de84563262e2ef82ed466e4dc5b5acbc4f562e410`.
- Cap USD 2,000; absolute application ceiling USD 2,500; USD only; retry limit 3; recheck 300 seconds.

See the [machine-readable evidence](final-verification-2026-08-31.json) for all retained fingerprints and timestamps.

## Scope and remaining submission work

This verifies the selected live hackathon workflow, not production readiness. No real commerce system, load/soak test, destructive outage experiment, or general multi-tenant security assessment was performed. Firestore concurrency edge cases are covered locally with a double plus live policy guards; this is not an exhaustive multi-instance isolation test.

The cloud tests used billable services with owner approval. No cost total is inferred from test success. Cloud services remain enabled; normal future development/rehearsals must return to the fake provider.

The following still needs owner action:

1. Record and upload the final English video to YouTube/Vimeo, at most four minutes, with an unedited live execution core and visible Google Cloud evidence. Use the corrected [demo script](demo-script.md); distinguish prior completed evidence from the live run.
2. Sign in to Devpost, select Taskmaster, fill the final fields using the [submission copy](devpost-submission.md), and attach the video, public code, architecture and live link.
3. Confirm eligibility, employer/ownership permission where relevant, contest-period creation, AI-assistant/prior-work disclosures, and third-party licensing.
4. Review judge access without publishing the administrator secret; verify all links signed out, submit, and preserve the submission confirmation.

The deadline remains **August 31, 2026 at 5 PM PDT / 7 PM Chicago**. [Official rules](https://allthingsagentichackathon.devpost.com/rules) were checked on August 31. No Devpost submission or final video upload was performed in this session.
