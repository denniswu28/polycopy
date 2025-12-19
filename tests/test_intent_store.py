import asyncio

import pytest

from polycopy.state import IntentStore


@pytest.mark.asyncio
async def test_intent_store_dedupes(tmp_path):
    db = tmp_path / "state.db"
    store = IntentStore(str(db))

    first = await store.record_intent_if_new("tx1", "intentA")
    second = await store.record_intent_if_new("tx1", "intentA")
    third = await store.record_intent_if_new("tx1", "intentB")

    assert first is True
    assert second is False
    assert third is True
    assert await store.seen("tx1", "intentA") is True
