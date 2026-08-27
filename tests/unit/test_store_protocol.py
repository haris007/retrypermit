import inspect

from retrypermit.adapters.stores.firestore import FirestoreStore
from retrypermit.adapters.stores.memory import MemoryStore
from retrypermit.ports.store import Store


def test_storage_adapters_implement_every_store_operation() -> None:
    required = {
        name
        for name, value in vars(Store).items()
        if inspect.isfunction(value) and not name.startswith("_")
    }
    for implementation in (MemoryStore, FirestoreStore):
        missing = sorted(
            name
            for name in required
            if not callable(getattr(implementation, name, None))
        )
        assert missing == [], f"{implementation.__name__} missing: {missing}"
