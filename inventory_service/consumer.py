import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from inventory_service.database import engine, Base, AsyncSessionLocal
from inventory_service.models import InventoryItem, ProcessedEvent

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092')
ORDER_TOPIC = "order-events"
INVENTORY_TOPIC = "inventory-events"
PAYMENT_TOPIC = "payment-events"

# Seed defaults configurable via environment (12-factor)
SEED_ITEM_NAME = os.getenv('SEED_ITEM_NAME', 'Mechanical Keyboard')
SEED_ITEM_STOCK = int(os.getenv('SEED_ITEM_STOCK', '10'))
SEED_ITEM_PRICE = float(os.getenv('SEED_ITEM_PRICE', '4500.0'))

async def init_db_and_seed_data():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(InventoryItem).where(InventoryItem.item_name == SEED_ITEM_NAME)
        )
        item = result.scalar_one_or_none()
        if not item:
            sample_item = InventoryItem(
                item_name=SEED_ITEM_NAME,
                available_stock=SEED_ITEM_STOCK,
                price=SEED_ITEM_PRICE
            )
            session.add(sample_item)
            await session.commit()
            print(f"📦 Initial stock seeded: {SEED_ITEM_NAME} (Stock: {SEED_ITEM_STOCK})")

async def process_order_event(event_data: dict, producer: AIOKafkaProducer):
    order_id = event_data.get("order_id")
    item_name = event_data.get("item_name")
    quantity = event_data.get("quantity", 1)
    # Dynamic pricing propagated from the order service (no hardcoded prices)
    price = event_data.get("price")
    total_amount = event_data.get("total_amount")
    if total_amount is None and price is not None:
        total_amount = float(price) * quantity
    event_unique_id = f"order_{order_id}_created"

    async with AsyncSessionLocal() as session:
        async with session.begin():
            # 1. ATOMIC DEDUPLICATION: Attempt insert first
            try:
                session.add(ProcessedEvent(event_id=event_unique_id))
                await session.flush()  # DB unique constraint triggers immediately if duplicate
            except IntegrityError:
                # Concurrent duplicate delivery safely trapped here
                print(f"⚠️ Event {event_unique_id} already processed (Atomic Dedup). Skipping.")
                return

            # 2. BUSINESS EFFECT: Row-level locked stock check
            query = (
                select(InventoryItem)
                .where(InventoryItem.item_name == item_name)
                .with_for_update()
            )
            result = await session.execute(query)
            item = result.scalar_one_or_none()

            if item and item.available_stock >= quantity:
                item.available_stock -= quantity
                # Both ProcessedEvent and Inventory update commit atomically at block exit
                print(f"✅ Stock Reserved for Order #{order_id}! Remaining Stock: {item.available_stock}")

                out_event = {
                    "event_type": "InventoryReserved",
                    "order_id": order_id,
                    "item_name": item_name,
                    "quantity": quantity,
                    "price": price,
                    "total_amount": total_amount,
                    "status": "STOCK_RESERVED"
                }
            else:
                print(f"❌ Insufficient Stock for Order #{order_id} ({item_name})!")
                out_event = {
                    "event_type": "InventoryFailed",
                    "order_id": order_id,
                    "reason": "OUT_OF_STOCK"
                }

        # DUAL-WRITE DECOUPLING: DB transaction commits first (block exit above),
        # only then publish the resulting event to Kafka.
        await producer.send_and_wait(INVENTORY_TOPIC, json.dumps(out_event).encode('utf-8'))

async def process_compensation_event(event_data: dict):
    event_type = event_data.get("event_type")
    if event_type != "PaymentFailed":
        return

    order_id = event_data.get("order_id")
    item_name = event_data.get("item_name")
    quantity = event_data.get("quantity", 1)
    event_unique_id = f"compensation_order_{order_id}"

    async with AsyncSessionLocal() as session:
        async with session.begin():
            # 1. ATOMIC DEDUPLICATION: Attempt insert first
            try:
                session.add(ProcessedEvent(event_id=event_unique_id))
                await session.flush()
            except IntegrityError:
                print(f"⚠️ Compensation for Order #{order_id} already applied (Atomic Dedup). Skipping.")
                return

            # 2. BUSINESS EFFECT: Restore stock with row lock
            query = (
                select(InventoryItem)
                .where(InventoryItem.item_name == item_name)
                .with_for_update()
            )
            result = await session.execute(query)
            item = result.scalar_one_or_none()

            if item:
                item.available_stock += quantity
                print(f"🔄 [Saga Compensation] Restored {quantity} units of '{item_name}' for Order #{order_id}. Current Stock: {item.available_stock}")

async def run_inventory_consumer():
    await init_db_and_seed_data()

    consumer = AIOKafkaConsumer(
        ORDER_TOPIC,
        PAYMENT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="inventory_service_group",
        auto_offset_reset="earliest",
        enable_auto_commit=False
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    await consumer.start()
    await producer.start()
    print("🎧 Inventory Consumer is listening on [order-events, payment-events]...")

    try:
        async for msg in consumer:
            event_payload = json.loads(msg.value.decode('utf-8'))
            if msg.topic == ORDER_TOPIC:
                await process_order_event(event_payload, producer)
            elif msg.topic == PAYMENT_TOPIC:
                await process_compensation_event(event_payload)

            # MANUAL COMMIT: acknowledge the offset only after processing completed
            await consumer.commit()
    except Exception as e:
        print(f"❌ Error in inventory consumer: {e}")
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(run_inventory_consumer())