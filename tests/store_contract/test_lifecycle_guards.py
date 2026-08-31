"""Lifecycle contract checks against MemoryStore and a non-network Firestore double.

The double checks read-before-write ordering and no commit on rejection. It does
not emulate Firestore isolation/retries; credentialed verification remains gated.
"""

from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pytest

from retrypermit.adapters.policies.seeded_json import SeededPolicyProvider
from retrypermit.adapters.stores import firestore as firestore_module
from retrypermit.adapters.stores.firestore import FirestoreStore, _dump
from retrypermit.adapters.stores.memory import MemoryStore
from retrypermit.domain.enums import PolicyStatus, RunStatus
from retrypermit.domain.errors import RunConflictError
from retrypermit.domain.runs import DemoRun


class Snapshot:
    def __init__(self, reference):
        self.reference = reference
        self.exists = reference.path in reference.client.documents
        self.data = deepcopy(reference.client.documents.get(reference.path))

    def to_dict(self):
        return deepcopy(self.data)


class Reference:
    def __init__(self, client, path, field_filter=None):
        self.client = client
        self.path = path
        self.field_filter = field_filter

    def collection(self, name):
        return Reference(self.client, f"{self.path}/{name}")

    def document(self, name):
        return Reference(self.client, f"{self.path}/{name}")

    def where(self, *, filter):
        return Reference(self.client, self.path, filter)

    async def get(self, *, transaction=None):
        if transaction:
            transaction.read(self.path)
        return Snapshot(self)


class Transaction:
    def __init__(self, client):
        self.client = client
        self.writes = []
        self.reads = []

    def read(self, path):
        assert not self.writes, "Firestore transaction read after write"
        self.reads.append(path)

    async def get(self, query):
        self.read(query.path)
        rows = []
        for path, data in self.client.documents.items():
            if path.rpartition("/")[0] != query.path:
                continue
            if (
                query.field_filter
                and data.get(query.field_filter.field_path) != query.field_filter.value
            ):
                continue
            rows.append(Snapshot(Reference(self.client, path)))

        async def stream():
            for row in rows:
                yield row

        return stream()

    def set(self, reference, data, merge=False):
        self.writes.append((reference.path, deepcopy(data), merge))

    def commit(self):
        for path, data, merge in self.writes:
            if merge:
                self.client.documents.setdefault(path, {}).update(data)
            else:
                self.client.documents[path] = data


class LocalFirestoreDouble:
    def __init__(self, documents):
        self.documents = documents
        self.transactions = []

    def collection(self, name):
        return Reference(self, name)

    def transaction(self):
        transaction = Transaction(self)
        self.transactions.append(transaction)
        return transaction


def local_transactional(operation):
    async def commit_on_success(transaction):
        result = await operation(transaction)
        transaction.commit()
        return result

    return commit_on_success


@pytest.fixture(params=["memory", "firestore-double"])
def store_factory(request, monkeypatch):
    async def create(status=RunStatus.READY, active=True):
        v1 = await SeededPolicyProvider().load()
        v1 = v1.model_copy(
            update={
                "status": PolicyStatus.ACTIVE,
                "activated_at": datetime.now(UTC),
                "activated_by": "local-contract-fixture",
            }
        )
        v2 = await SeededPolicyProvider(
            Path("fixtures/policies/runbook-v2.json")
        ).load()
        now = datetime.now(UTC)
        run = DemoRun(
            run_id="lifecycle-contract",
            tenant_id=v1.tenant_id,
            status=status,
            active=active,
            active_policy_version=v1.version,
            created_at=now,
            updated_at=now,
        )
        if request.param == "memory":
            store = MemoryStore()
            store._policies[(v1.tenant_id, v1.version)] = v1
            store._policies[(v2.tenant_id, v2.version)] = v2
            store._active_policy[v1.tenant_id] = v1.version
            store._runs[run.run_id] = run
        else:
            monkeypatch.setattr(
                firestore_module, "async_transactional", local_transactional
            )
            client = LocalFirestoreDouble(
                {
                    f"tenants/{v1.tenant_id}": {"active_policy_version": v1.version},
                    f"tenants/{v1.tenant_id}/policy_versions/{v1.version}": _dump(v1),
                    f"tenants/{v2.tenant_id}/policy_versions/{v2.version}": _dump(v2),
                    f"demo_runs/{run.run_id}": _dump(run),
                }
            )
            store = FirestoreStore(client=client)
        return store, run, v2

    return create


@pytest.mark.asyncio
async def test_running_run_blocks_new_policy_but_same_version_is_idempotent(
    store_factory,
):
    store, run, v2 = await store_factory(RunStatus.RUNNING)
    with pytest.raises(RunConflictError, match="until the active run completes"):
        await store.activate_policy(run.tenant_id, v2.version, "local-test")
    assert (
        await store.get_active_policy_version(run.tenant_id)
        == run.active_policy_version
    )
    assert (await store.get_run(run.run_id)).status == RunStatus.RUNNING
    assert (
        await store.activate_policy(
            run.tenant_id, run.active_policy_version, "local-test"
        )
    ).version == run.active_policy_version


@pytest.mark.asyncio
async def test_ready_policy_change_fences_stale_start_without_changing_failure_mode(
    store_factory,
):
    store, run, v2 = await store_factory()
    await store.activate_policy(run.tenant_id, v2.version, "local-test")
    with pytest.raises(RunConflictError, match="reset before starting"):
        await store.mark_run_running(run.run_id, inject_failure=True)
    unchanged = await store.get_run(run.run_id)
    assert unchanged.status == RunStatus.READY
    assert unchanged.inject_failure is False
    assert unchanged.started_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,active", [(RunStatus.COMPLETE, True), (RunStatus.INACTIVE, False)]
)
async def test_completed_or_inactive_run_does_not_block_policy_change(
    store_factory, status, active
):
    store, run, v2 = await store_factory(status, active)
    await store.activate_policy(run.tenant_id, v2.version, "local-test")
    assert await store.get_active_policy_version(run.tenant_id) == v2.version


@pytest.mark.asyncio
@pytest.mark.parametrize("inject_failure", [False, True])
async def test_start_and_resume_atomically_preserve_mode_and_start_time(
    store_factory, inject_failure
):
    store, run, _ = await store_factory()
    started = await store.mark_run_running(run.run_id, inject_failure=inject_failure)
    assert started.status == RunStatus.RUNNING
    assert started.inject_failure is inject_failure
    resumed = await store.mark_run_running(run.run_id, inject_failure=inject_failure)
    assert resumed.started_at == started.started_at
    worker_start = await store.mark_run_running(run.run_id)
    assert worker_start.inject_failure is inject_failure
    with pytest.raises(RunConflictError, match="retain its failure-injection"):
        await store.mark_run_running(run.run_id, inject_failure=not inject_failure)
    assert (await store.get_run(run.run_id)).inject_failure is inject_failure


@pytest.mark.asyncio
async def test_inactive_run_cannot_start(store_factory):
    store, run, _ = await store_factory(RunStatus.INACTIVE, False)
    with pytest.raises(RunConflictError, match="inactive runs"):
        await store.mark_run_running(run.run_id, inject_failure=True)
