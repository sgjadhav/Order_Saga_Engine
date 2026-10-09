import asyncio
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

# Reusing your existing project's engine and configuration directly
from inventory_service.database import engine
from inventory_service.models import Base, InventoryItem

async def simulate_reservation(session_factory, item_name: str, quantity: int) -> bool:
    """Simulates the reservation logic with row-level lock (.with_for_update())."""
    async with session_factory() as session:
        async with session.begin():
            # Pessimistic lock row-level check
            query = (
                select(InventoryItem)
                .where(InventoryItem.item_name == item_name)
                .with_for_update()
            )
            result = await session.execute(query)
            item = result.scalar_one_or_none()

            if item and item.available_stock >= quantity:
                # Artificial tiny delay to simulate realistic I/O contention
                await asyncio.sleep(0.01)
                item.available_stock -= quantity
                return True
            return False

@pytest.mark.asyncio
async def test_concurrent_inventory_reservations():
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # 1. Reset database state & seed exactly 10 units
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        keyboard = InventoryItem(item_name="Mechanical Keyboard", available_stock=10, price=4500.0)
        session.add(keyboard)
        await session.commit()

    # 2. Fire 10 concurrent requests simultaneously, each demanding 3 units (Total demand = 30 units)
    tasks = [simulate_reservation(session_factory, "Mechanical Keyboard", 3) for _ in range(10)]
    results = await asyncio.gather(*tasks)

    successful = [r for r in results if r is True]
    failed = [r for r in results if r is False]

    # 3. Assertions
    # With 10 stock, only 3 orders of 3 units (9 total) can succeed. The 4th must fail.
    assert len(successful) == 3, f"Expected 3 successful reservations, got {len(successful)}"
    assert len(failed) == 7, f"Expected 7 failed reservations, got {len(failed)}"

    # 4. Verify remaining database stock
    async with session_factory() as session:
        res = await session.execute(
            select(InventoryItem).where(InventoryItem.item_name == "Mechanical Keyboard")
        )
        final_stock = res.scalar_one().available_stock
        assert final_stock == 1, f"Remaining stock must be 1, but got {final_stock}"

    print("\n✅ Concurrency test passed: 0 overselling detected under high contention!")

if __name__ == "__main__":
    asyncio.run(test_concurrent_inventory_reservations())