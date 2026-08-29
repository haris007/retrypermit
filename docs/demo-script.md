# RetryPermit four-minute demo script

This is the Phase 5 rehearsal script. Rehearse locally with the deterministic fixture. The final recording must be made only after the owner authorizes cloud verification and the live Phase 3/4 acceptance run passes.

## Before recording

- Use the dedicated RetryPermit Google Cloud project; do not show local mode as cloud evidence.
- Verify the live `/health` response says `Google Cloud`, `Firestore`, and `Google ADK / ...`.
- Reset to a fresh synthetic run and close unrelated tabs and notifications.
- Set browser zoom so counters, state, policy clause, receipts, and effect proof are readable.
- Keep the Google Cloud console tabs ready: Cloud Run, Pub/Sub subscription, Firestore, and Logs.
- Record a backup and test the public video signed out.

## Important timing decision before the final recording

The approved production v1/v2 policies currently specify a 300-second transient recheck. The local 10-second interval is explicitly a demo-only override and is disabled in real cloud mode. A fresh cloud run therefore cannot show Class B defer-and-recover inside a four-minute video.

Before final cloud verification, the owner must choose and document one truthful approach:

1. Approve a separate, clearly named synthetic hackathon demo policy with a 10-second recheck, using the existing audited policy approval/activation mechanism and leaving production policies unchanged; or
2. Keep the 300-second policy, show the live initial Class A/C/D decisions unedited, and then show a previously completed cloud run for Class B recovery with timestamps visible.

This is a submission/demo choice, not authorization to modify or deploy anything. The current local Phase 5 work does not make that choice on the owner's behalf.

## Timed narration and screen actions

| Time | Screen action | Narration |
| --- | --- | --- |
| 0:00–0:20 | Operations console, fresh 12-message synthetic DLQ | “Every item here is a synthetic order that did not complete. Normally an on-call engineer would inspect, repair, retry, and document each one by hand.” |
| 0:20–0:40 | Show disclosures and active policy | “RetryPermit repairs only what an approved runbook permits. Every external attempt gets a receipt, and duplicate delivery is safe because the business effect is idempotent.” |
| 0:40–1:55 | Start run; select Class A then B then C while counters move | “Six schema-drift events can be repaired through allowlisted rename and type coercion. Three valid orders meet a transient inventory 503 and receive their own scheduled recheck. Negative quantity and disallowed currency are business decisions, so the system proposes a correction but withholds replay and escalates.” |
| 1:55–2:15 | Select message 12, show instruction flag, `QUARANTINED`, refusal receipt | “This order note tells the agent to ignore policy. It is treated as inert data. The model cannot expand its authority, and deterministic code quarantines the message with no downstream effect.” |
| 2:15–2:40 | Fresh failure run; click lost-response proof; show 2 attempts → 1 effect | “Here the simulated downstream commits the effect, then its response is lost. RetryPermit keeps the same payload-bound key. The retry returns the original reference: two attempts, one unique business effect.” |
| 2:40–3:05 | Policy panel: hash, approval/activation events, cap; show historical v1/v2 evidence | “Policy is versioned, hashed, approved, and activated separately. PDF extraction has no authority. In the Phase 2 policy fixture, v2 changed the same 750-dollar decision without a code change, while the application’s absolute ceiling still wins.” |
| 3:05–3:30 | Security boundary diagram or decision/receipts panel | “Gemini through Google ADK classifies, cites policy, and proposes a repair. It cannot publish, write state, mint an idempotency key, activate policy, or authorize anything outside the runbook. Deterministic code owns every side effect.” |
| 3:30–3:47 | Cloud Run revision, Pub/Sub push subscription, Firestore run/effect, structured logs | “The agent and deterministic orchestrator run in Cloud Run. Pub/Sub delivers at least once, Cloud Tasks schedules each message, Firestore preserves state and proof, and Scheduler is the recovery net.” |
| 3:47–3:58 | Return to final counters/proof | “RetryPermit removes repetitive 3 AM queue remediation while preserving the safety boundaries an experienced on-call engineer expects.” |

## Evidence that must be readable

- Twelve messages and the final/current counters.
- At least one repair diff with an approved policy clause.
- Class B `DEFERRED` and its `recheck_at`, plus cloud-complete evidence if approach 2 is used.
- Class C escalation with proposed fix and “replay withheld.”
- Class D refusal/quarantine and zero effect.
- Lost-response proof: two execution attempts, two downstream requests, one unique effect, confirmed original reference.
- Active policy hash, status, effective cap, approval actor, and activation event.
- Cloud Run service/revision, authenticated Pub/Sub subscription, Firestore documents, and structured logs.

## Local rehearsal commands

Use the required local configuration and the normal local setup in the README. The reusable Phase 4 freeze gate exercises clean v1, injected lost-response v1, and selected v2 at real local cadence. It must remain local and makes no cloud claim.

## Recording truth rules

- Say “at-least-once delivery with effectively-once downstream business effects,” never “exactly once.”
- Say “simulated downstream” every time its proof is introduced.
- State that every order, customer, policy, and recipient is synthetic.
- Never describe local screenshots as Google Cloud evidence.
- Do not claim the optional duplicate-redelivery control exists; deduplication is proven by tests.
- Do not claim the current 12-message run demonstrates a v1/v2 decision change; that proof uses the six-message Phase 2 policy fixture unless the dataset is deliberately changed after the freeze.
