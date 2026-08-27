#!/usr/bin/env python3
"""Guarded recursive cleanup for expired RetryPermit synthetic demo runs."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter


CONFIRMATION = "delete-expired-synthetic-runs"


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("use an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return parsed.astimezone(UTC)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "List or recursively delete inactive synthetic runs whose 30-day "
            "retention timestamp has passed. Dry-run is the default."
        )
    )
    parser.add_argument("--project", required=True)
    parser.add_argument("--before", type=_timestamp, default=datetime.now(UTC))
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    return parser


def main() -> int:
    args = _parser().parse_args()
    if "retrypermit" not in args.project.lower():
        raise SystemExit(
            "Refusing cleanup: the dedicated project id must contain 'retrypermit'."
        )
    if not 1 <= args.limit <= 500:
        raise SystemExit("--limit must be between 1 and 500")
    if args.execute and args.confirm != CONFIRMATION:
        raise SystemExit(
            f"Refusing deletion: pass --confirm={CONFIRMATION} with --execute."
        )

    client = firestore.Client(project=args.project)
    query = (
        client.collection("demo_runs")
        .where(filter=FieldFilter("expires_at", "<=", args.before))
        .limit(args.limit)
    )
    candidates = []
    for snapshot in query.stream():
        data = snapshot.to_dict() or {}
        if data.get("active") is False and data.get("tenant_id") == "synthetic-demo":
            candidates.append(snapshot)

    action = "DELETE" if args.execute else "DRY-RUN"
    for snapshot in candidates:
        data = snapshot.to_dict() or {}
        print(f"{action} {snapshot.reference.path} expires_at={data.get('expires_at')}")
        if args.execute:
            deleted = client.recursive_delete(snapshot.reference)
            print(f"DELETED {snapshot.reference.path} documents={deleted}")

    print(f"{action} candidates={len(candidates)} project={args.project}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
