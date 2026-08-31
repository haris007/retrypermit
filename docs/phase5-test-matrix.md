# Phase 5 test matrix

This matrix maps the Phase 5 acceptance checklist to executable tests. The normal suite runs locally with the deterministic model, in-memory store, and no Google Cloud credentials. The two deployed/model rows remain intentionally skipped until the owner authorizes final cloud verification.

## Policy

| Requirement | Test evidence |
| --- | --- |
| PDF extraction | `tests/unit/test_phase2_failure_and_policy.py::test_pdf_policy_requires_explicit_approval_and_activation` |
| Schema validation | `tests/unit/test_policy_and_repair.py::test_policy_cap_is_clamped_and_invalid_clause_repair_is_rejected` |
| Unauthorized/unapproved activation rejected | `tests/api/test_phase2_api.py::test_pdf_extract_approve_activate_routes_are_admin_guarded`; unit lifecycle test |
| Absolute safety cap clamps over-limit policy | `tests/unit/test_policy_and_repair.py::test_policy_cap_is_clamped_and_invalid_clause_repair_is_rejected`; `test_system_cap_remains_2500_for_any_policy` |
| Runbook-embedded instruction cannot alter behavior | `tests/unit/test_phase2_failure_and_policy.py::test_malformed_pdf_and_embedded_instructions_fail_safe` |

## Classification and repair

| Requirement | Test evidence |
| --- | --- |
| Schema-drift classification | `tests/unit/test_policy_versions_and_hashing.py::test_fake_provider_classifies_the_approved_schema_drift` |
| Allowlisted rename and type coercion | `tests/unit/test_policy_and_repair.py::test_allowlisted_repair_and_invented_transform_rejection` |
| Invented transform rejected | same test plus invalid-clause policy test |
| Low-confidence escalation | `tests/unit/test_orchestrator.py::test_low_confidence_proposal_escalates_without_an_effect` |
| Invalid currency and negative quantity escalation | `tests/unit/test_phase3_outcomes.py::test_class_c_records_withheld_escalation_and_never_replays` |
| Prompt-injection quarantine | `tests/unit/test_phase3_outcomes.py::test_class_d_is_quarantined_with_refusal_and_inert_payload_text` |

## Reliability

| Requirement | Test evidence |
| --- | --- |
| Transient deferral, per-message recheck, resume | `tests/unit/test_phase3_outcomes.py::test_class_b_defers_rechecks_twice_then_replays_automatically` |
| Duplicate Pub/Sub delivery | `tests/api/test_api_and_delivery.py::test_duplicate_pubsub_push_and_task_produce_one_downstream_effect`; memory-store transaction test |
| Replay-ledger transitions | `tests/unit/test_orchestrator.py::test_phase3_initial_pass_routes_all_four_classes_without_unsafe_effects`; store atomicity test |
| Payload-hash mismatch rejected | `tests/store_contract/test_memory_invariants.py::test_same_key_with_different_payload_hash_fails_safely` |
| Expiring-lease recovery | `tests/store_contract/test_memory_invariants.py::test_abandoned_lease_recovers_and_downstream_effect_stays_unique` |
| 503 and timeout recovery | `tests/unit/test_phase2_failure_and_policy.py` injected-503 and timeout tests |
| Same key on retry; two attempts, one effect | injected-503 test and `tests/api/test_phase2_api.py::test_start_with_failure_and_live_proof` |
| No replay above effective cap | policy cap tests and `test_over_500_escalates_under_v1_and_replays_under_active_v2` |
| `failed_stage` routing | `tests/store_contract/test_memory_invariants.py::test_failed_stage_cannot_route_triage_failure_into_replay` |
| Durable acknowledgement/task coordination | `tests/api/test_api_and_delivery.py::test_pubsub_ack_waits_for_durable_task_schedule` |

## System and UI safety

| Requirement | Test evidence |
| --- | --- |
| Health, admin auth, reset, history | `tests/api/test_api_and_delivery.py::test_health_admin_auth_reset_and_history_preservation` |
| Complete 12-message end-to-end run | `tests/api/test_api_and_delivery.py::test_api_start_runs_twelve_messages_end_to_end` |
| Four-class 6/3/2/1 distribution | `tests/unit/test_phase3_outcomes.py::test_phase3_fixture_distribution_is_six_three_two_one` |
| Bounded model concurrency | `tests/unit/test_phase4_performance.py::test_triage_is_concurrent_but_bounded_at_four` |
| Policy cache invalidation | `tests/unit/test_phase4_performance.py::test_active_policy_is_cached_and_explicitly_invalidated` |
| No raw HTML injection sink | `tests/unit/test_phase3_outcomes.py::test_frontend_has_no_raw_html_injection_sink` |

## Submission-readiness regression checks (August 31)

The normal suite now reports **66 passed, 2 skipped**. Network connections were blocked during local verification. The new `tests/api/test_v3_runbook.py` covers exact fixture selection, full v3 approval/activation, both run modes, genuine deferred/recheck transitions, and active-policy context sent to the model.

| Requirement | Test evidence |
| --- | --- |
| Reject a policy switch while a message is deferred or an effect is ambiguous; retain resumability | `tests/api/test_submission_reliability.py` policy-switch tests |
| Revalidate a second worker's cached policy; fence a stale READY run | `test_second_worker_revalidates_cached_policy_and_ready_run_must_reset` |
| Preserve original mode and start time after partial publication; no duplicate effects | `test_start_resumes_after_partial_publication_and_preserves_effects`, both injection modes |
| Start and policy activation cannot both win | `test_start_and_policy_activation_cannot_both_win`, both operation orders |
| Memory/Firestore lifecycle contracts and read-before-write ordering | `tests/store_contract/test_lifecycle_guards.py` |
| Cache immutable definitions while checking current authority | Updated `tests/unit/test_phase4_performance.py` cache test |

The Firestore lifecycle tests use a non-network transaction double. Real Firestore isolation, cloud transport, and the live model still require the gated integration tests. Read-only inspection of the August 29 cloud revision did not exercise these newly fixed paths.

## Gated integration checks

| Test | Why it cannot run locally |
| --- | --- |
| `tests/deployed/test_deployed_vertical_slice.py` | Requires a deployed URL, administrator secret, Google Cloud project, real Pub/Sub/Tasks/Firestore/OIDC, and billable ADK/Gemini use. It now asserts the frozen 12-message 9/2/1 outcome and permits the production five-minute recheck. |
| `tests/deployed/test_real_gemini.py` | Requires real Application Default Credentials and a billable Vertex AI Gemini call. |

These remain expected skips in the normal local suite, not local passes. The owner authorized and completed the August 31 live session. Real Gemini triage passed; both fresh deployed workflows completed. A test-only archived-status assertion was corrected, then the same runs passed read-only revalidation (`1 passed in 12.64s`) including independent Firestore evidence. The initial command's failure is retained in the [full report](final-verification-2026-08-31.md).

To recheck only those persisted runs during an explicitly authorized read-only cloud session, set `RETRYPERMIT_VERIFY_EXISTING_RUN_IDS` to the comma-separated v1/v3 run IDs and run only `tests/deployed/test_deployed_vertical_slice.py`. This mode performs no new model calls or mutations and must never be described as a fresh live execution. Leave it unset for fresh acceptance. Local v3 tests also verify reset archives a completed run while preserving its completion timestamp and nine effects.
