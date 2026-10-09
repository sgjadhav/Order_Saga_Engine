import asyncio
import pytest
from unittest.mock import AsyncMock
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker
from inventory_service.database import engine
from inventory_service.models import Base, InventoryItem, ProcessedEvent
from inventory_service.consumer import process_order_event

@pytest.mark.asyncio
async def test_atomic_idempotency_duplicate_messages():
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # 1. Reset inventory DB and seed exactly 10 units
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        item = InventoryItem(item_name="Mechanical Keyboard", available_stock=10, price=4500.0)
        session.add(item)
        await session.commit()

    # Mock producer to avoid real Redpanda broker calls during unit test
    mock_producer = AsyncMock()
    mock_producer.send_and_wait = AsyncMock()

    # Simulating Kafka redelivering the exact same event 5 times at the exact same moment
    duplicate_payload = {
        "order_id": 999,
        "item_name": "Mechanical Keyboard",
        "quantity": 2
    }

    # 2. Fire 5 duplicate events concurrently
    tasks = [process_order_event(duplicate_payload, mock_producer) for _ in range(5)]
    await asyncio.gather(*tasks)

    # 3. Assertions
    async with session_factory() as session:
        # Stock should deduct ONLY ONCE: 10 - 2 = 8
        res_item = await session.execute(
            select(InventoryItem).where(InventoryItem.item_name == "Mechanical Keyboard")
        )
        final_stock = res_item.scalar_one().available_stock
        assert final_stock == 8, f"Stock should be 8, but got {final_stock}!"

        # ProcessedEvent table should have ONLY 1 entry for this event ID
        res_events = await session.execute(select(ProcessedEvent))
        events = res_events.scalars().all()
        assert len(events) == 1, f"Expected 1 processed_event record, found {len(events)}"
        assert events[0].event_id == "order_999_created"

    print("\n✅ Atomic Idempotency Test Passed: Exactly-once business effect guaranteed!")

if __name__ == "__main__":
    asyncio.run(test_atomic_idempotency_duplicate_messages())