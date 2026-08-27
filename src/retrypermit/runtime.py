from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from retrypermit.adapters.models.adk_gemini import AdkGeminiProvider
from retrypermit.adapters.models.fake import DeterministicFakeModelProvider
from retrypermit.adapters.policies.seeded_json import SeededPolicyProvider
from retrypermit.adapters.pubsub.google_publisher import (
    GooglePubSubPublisher,
    RecordingPublisher,
)
from retrypermit.adapters.stores.firestore import FirestoreStore
from retrypermit.adapters.stores.memory import MemoryStore
from retrypermit.adapters.tasks.cloud_tasks import (
    CloudTaskScheduler,
    RecordingTaskScheduler,
)
from retrypermit.application.delivery_service import DeliveryService
from retrypermit.application.demo_service import DemoService
from retrypermit.application.orchestrator import RetryPermitOrchestrator
from retrypermit.config import Settings
from retrypermit.domain.errors import NotFoundError
from retrypermit.ports.publisher import Publisher
from retrypermit.ports.store import Store
from retrypermit.ports.task_scheduler import TaskScheduler


@dataclass(slots=True)
class Runtime:
    settings: Settings
    store: Store
    demo: DemoService
    orchestrator: RetryPermitOrchestrator
    delivery: DeliveryService
    publisher: Publisher
    scheduler: TaskScheduler


async def build_runtime(settings: Settings) -> Runtime:
    if settings.use_in_memory_store:
        store: Store = MemoryStore()
    else:
        store = FirestoreStore(project=settings.google_cloud_project)

    policy_provider = SeededPolicyProvider(settings.policy_fixture_path)
    demo = DemoService(store, policy_provider)
    await demo.bootstrap()

    if settings.use_fake_model:
        model_provider = DeterministicFakeModelProvider()
    else:
        model_provider = AdkGeminiProvider(settings.gemini_model)
    orchestrator = RetryPermitOrchestrator(
        store,
        model_provider,
        lease_duration=timedelta(seconds=settings.replay_lease_seconds),
        model_timeout_seconds=settings.model_timeout_seconds,
    )

    if settings.use_in_memory_store:
        publisher: Publisher = RecordingPublisher()
        scheduler: TaskScheduler = RecordingTaskScheduler()
    else:
        publisher = GooglePubSubPublisher(settings)
        scheduler = CloudTaskScheduler(settings)
    delivery = DeliveryService(
        store,
        scheduler,
        orchestrator,
        inbox_lease=timedelta(seconds=settings.inbox_lease_seconds),
    )

    runtime = Runtime(
        settings=settings,
        store=store,
        demo=demo,
        orchestrator=orchestrator,
        delivery=delivery,
        publisher=publisher,
        scheduler=scheduler,
    )

    # Local convenience mode starts with one visibly synthetic, in-memory run. Cloud
    # mode never mutates run state merely because another instance cold-started.
    if settings.demo_mode and settings.use_in_memory_store:
        try:
            await demo.get_active()
        except NotFoundError:
            await demo.reset()
    return runtime
