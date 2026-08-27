"""Simulated downstream adapters with an independent idempotency boundary."""

from retrypermit.adapters.downstream.store_backed import StoreBackedDownstream

__all__ = ["StoreBackedDownstream"]
