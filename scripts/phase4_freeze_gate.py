"""Run the Phase 4 deterministic feature-freeze gate.

This intentionally uses the same ten-second recheck cadence as the local demo.
It never enables a cloud adapter or reads Google Cloud credentials.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from retrypermit.config import Settings
from retrypermit.main import create_app


ADMIN_TOKEN = "phase4-freeze-gate-local-only"
ADMIN_HEADERS = {"X-Admin-Token": ADMIN_TOKEN}
EXPECTED_OUTCOME = {
    "replayed_count": 9,
    "deferred_total": 3,
    "escalated_count": 2,
    "quarantined_count": 1,
    "dlq_depth": 3,
}


def _require_ok(response: Any, action: str) -> dict[str, Any]:
    if response.status_code >= 400:
        raise AssertionError(f"{action} failed: {response.status_code} {response.text}")
    return response.json()


def _run_scenario(
    client: TestClient, *, name: str, endpoint: str, expected_policy: str
) -> dict[str, Any]:
    _require_ok(client.post("/api/demo/reset", headers=ADMIN_HEADERS), f"{name} reset")
    started = time.perf_counter()
    result = _require_ok(client.post(endpoint, headers=ADMIN_HEADERS), f"{name} start")
    wall_seconds = round(time.perf_counter() - started, 3)
    stats = _require_ok(client.get("/api/stats"), f"{name} stats")
    outcome = {key: stats[key] for key in EXPECTED_OUTCOME}

    assert result["status"] == "COMPLETE", result
    assert stats["run_status"] == "COMPLETE", stats
    assert stats["active_policy_version"] == expected_policy, stats
    assert stats["model"] == "deterministic-fixture", stats
    assert stats["mode"] == "Local / in-memory", stats
    assert wall_seconds < 90, wall_seconds
    assert outcome == EXPECTED_OUTCOME, outcome

    return {
        "scenario": name,
        "run_id": result["run_id"],
        "policy": stats["active_policy_version"],
        "wall_seconds": wall_seconds,
        "reported_elapsed_seconds": stats["elapsed_seconds"],
        "outcome": outcome,
    }


def main() -> None:
    settings = Settings(
        _env_file=None,
        demo_admin_token=ADMIN_TOKEN,
        allow_local_service_auth=True,
    )
    assert settings.app_env == "local"
    assert settings.demo_mode is True
    assert settings.use_in_memory_store is True
    assert settings.use_fake_model is True
    assert settings.mode_label == "Local / in-memory"

    with TestClient(create_app(settings)) as client:
        health = _require_ok(client.get("/health"), "health")
        assert health["model_provider"] == "deterministic fixture", health
        assert health["store"] == "in-memory (non-durable)", health

        runs = [
            _run_scenario(
                client,
                name="clean-v1",
                endpoint="/api/demo/start",
                expected_policy="runbook-v1",
            ),
            _run_scenario(
                client,
                name="lost-response-v1",
                endpoint="/api/demo/start-with-failure",
                expected_policy="runbook-v1",
            ),
        ]

        pdf_path = Path("fixtures/dlq-runbook-v2.pdf")
        with pdf_path.open("rb") as pdf_file:
            extracted = _require_ok(
                client.post(
                    "/api/policies/extract",
                    headers=ADMIN_HEADERS,
                    files={"file": (pdf_path.name, pdf_file, "application/pdf")},
                ),
                "extract v2",
            )
        assert extracted["policy"]["definition"]["version"] == "runbook-v2"
        _require_ok(
            client.post(
                "/api/policies/runbook-v2/approve",
                headers=ADMIN_HEADERS,
                json={"actor": "phase4-freeze-gate"},
            ),
            "approve v2",
        )
        _require_ok(
            client.post(
                "/api/policies/runbook-v2/activate",
                headers=ADMIN_HEADERS,
                json={"actor": "phase4-freeze-gate"},
            ),
            "activate v2",
        )
        runs.append(
            _run_scenario(
                client,
                name="selected-v2",
                endpoint="/api/demo/start",
                expected_policy="runbook-v2",
            )
        )

    outcomes = [run["outcome"] for run in runs]
    assert outcomes[1:] == outcomes[:-1], outcomes
    print(
        json.dumps(
            {
                "gate": "PASS",
                "configuration": {
                    "mode": settings.mode_label,
                    "store": "in-memory",
                    "model": "deterministic fixture",
                    "google_cloud_calls": 0,
                },
                "runs": runs,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
