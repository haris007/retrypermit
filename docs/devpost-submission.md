# Devpost submission draft

## Project name

RetryPermit — policy-governed recovery for failed events

## Category

**Taskmaster** (exactly one category)

## The recurring loss

When an event-driven order fails, the expensive part is rarely pressing “retry.” An experienced on-call engineer must reconstruct what happened, decide whether the runbook permits a repair, avoid duplicating an effect whose response may have been lost, wait for transient dependencies, and leave enough evidence for the next person. That repeated 3 AM investigation is the operational loss RetryPermit removes.

## What RetryPermit does

RetryPermit receives a synthetic dead-letter event and autonomously carries it through a durable state machine. Gemini through Google ADK proposes a structured classification, concise evidence, a policy clause, and an allowlisted repair. Deterministic code validates the proposal against a hashed, approved active runbook and chooses one of four outcomes:

- repair schema drift and replay;
- defer a transient dependency failure and schedule a per-message recheck;
- escalate unsafe business values with a proposed correction but no replay; or
- quarantine instruction-like content and record a refusal.

Every transport attempt, model call, task schedule, transition, replay, and result becomes an auditable receipt. The verified result is visible as final counters, state history, policy evidence, and a separately queryable downstream-effect record.

## How it is autonomous

The agent works in the background after the operator starts a run. Pub/Sub provides at-least-once event delivery. A durable Firestore inbox and deterministic Cloud Task carry each message into a worker. The state machine persists progress, leases allow work to resume after interruption, future tasks recheck transient failures, and Cloud Scheduler repairs missed or stale work. Cases that require business judgment are escalated; adversarial content is quarantined. No person has to keep a browser request open for the workflow to continue.

## The safety boundary

**The model proposes; deterministic code executes.** The ADK agent has no side-effecting tools. It cannot publish, write Firestore, activate policy, mint an idempotency key, or call the downstream. Strict application code validates schemas, policy hashes, clauses, allowlisted transforms, confidence, retry budgets, payload hashes, and the absolute safety cap before any effect.

## How it was built

RetryPermit uses one Cloud Run service for the hackathon vertical slice. It hosts the React operations console, FastAPI API, authenticated Pub/Sub ingress, Cloud Tasks worker, recovery endpoint, deterministic orchestrator, and simulated downstream. Firestore stores the durable inbox, state transitions, receipts, policy lifecycle, replay ledger, and effect proof. Cloud Tasks provides one named task per message and future rechecks; Cloud Scheduler is only the recovery net. Dedicated service accounts and OIDC tokens protect machine routes. Secret Manager supplies the administrator token. Google ADK structures the agent interaction with Gemini on Vertex AI.

## Proof under an ambiguous outcome

The most important demo deliberately creates the failure distributed systems fear: the simulated downstream commits an order effect, then the response is lost. RetryPermit records a retryable replay failure and retries with the same payload-bound idempotency key. The downstream returns the original reference. The proof view and independent store record show two execution attempts, two downstream requests, and one unique business effect.

The precise promise is **at-least-once delivery with effectively-once downstream business effects**. RetryPermit does not claim exactly-once delivery.

## Engineering tradeoff and learning

For a short hackathon sprint, the API, worker, and simulated downstream share one Cloud Run service. That makes deployment, cold starts, logs, and demonstration simpler while real Pub/Sub, Cloud Tasks, Firestore, OIDC, and Vertex AI boundaries remain visible. In production, those trust and scaling domains should be split.

The key learning was that the model is most useful when its authority is deliberately small. Classification, evidence, and repair proposals benefit from Gemini; state transitions, policy authority, idempotency, and effects benefit from deterministic code. Reliability came from making every uncertain boundary explicit rather than asking the model to “handle retries.”

## Limitations and disclosures

All messages, customers, products, policy documents, recipients, and outcomes are synthetic. The downstream is an in-service idempotent order-processor simulation; no real commerce platform is contacted. Local development uses an in-memory store, a recording task scheduler, and a deterministic fake model and is not evidence of Google Cloud durability. The current browser does not expose a manual duplicate-Pub/Sub-redelivery control; duplicate delivery is covered by automated tests.

The hackathon deployment intentionally limits Cloud Run to one instance and uses a public synthetic read UI protected by route-specific authorization for mutations and machine calls. Production work remains: integrate a real downstream, split services, add organization-grade monitoring and incident response, validate retention/privacy requirements, conduct a security review, and complete commercial/name clearance.

## Submission fields still requiring owner evidence

- **Live project URL:** add only after final cloud verification; do not invent. The currently deployed revision still serves the six-message Phase 2 dataset, so it must not be submitted as evidence of the frozen twelve-message behaviour.
- **Repository URL:** `https://github.com/haris007/retrypermit` — public and MIT licensed. Reopen it signed out after the final push and confirm the submitted branch contains the phase you describe.
- **Public video URL:** add after recording; verify while signed out and confirm duration is no more than four minutes.
- **Contest-period statement:** owner must confirm the project was newly created during the eligible submission period, that AI coding assistants were used, and that no undisclosed pre-existing code was incorporated.
- **Eligibility/employer approval:** owner confirmation required.

## Optional published-content draft

The project’s reusable lesson is simple: an AI agent should not own irreversible recovery actions merely because it can explain them. RetryPermit gives Gemini the work it is good at—classification, concise evidence, and policy-grounded proposals—then puts deterministic schemas, hashes, state machines, caps, leases, and idempotency between that proposal and every effect. The result tolerates duplicate delivery and even a lost response after a committed effect while still producing one auditable business outcome.

## Optional social post draft

I built RetryPermit for the #AllThingsAgenticHackathon: a policy-governed recovery agent that triages failed events, schedules transient rechecks, escalates unsafe cases, quarantines instruction-like payloads, and proves two retry attempts can still produce one idempotent business effect. Gemini proposes; deterministic code executes. Synthetic data and a simulated downstream keep the demo safe.
