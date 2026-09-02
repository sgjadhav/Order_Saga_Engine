import asyncio
import json
from aiokafka import AIOKafkaConsumer
from sqlalchemy import update, select
from order_service.database import AsyncSessionLocal
from order_service.models import Order

KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
PAYMENT_TOPIC = "payment-events"

async def update_order_status(event_data: dict):
    event_type = event_data.get("event_type")
    order_id = event_data.get("order_id")

    if not order_id or event_type not in ["PaymentCompleted", "PaymentFailed"]:
        return

    new_status = "CONFIRMED" if event_type == "PaymentCompleted" else "CANCELLED"

    async with AsyncSessionLocal() as session:
        # Check current order state
        query = select(Order).where(Order.id == order_id)
        result = await session.execute(query)
        order = result.scalar_one_or_none()

        if order:
            order.status = new_status
            await session.commit()
            print(f"📦 Order #{order_id} status updated to -> [{new_status}]")
        else:
            print(f"⚠️ Order #{order_id} not found in database.")

async def run_order_status_consumer():
    consumer = AIOKafkaConsumer(
        PAYMENT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="order_status_update_group",
        auto_offset_reset="earliest"
    )

    await consumer.start()
    print("📋 Order Status Consumer is listening for payment events on Redpanda/Kafka...")

    try:
        async for msg in consumer:
            event_payload = json.loads(msg.value.decode("utf-8"))
            print(f"📥 [Order Update] Received event: {event_payload}")
            await update_order_status(event_payload)
    except Exception as e:
        print(f"❌ Error in order status consumer: {e}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(run_order_status_consumer())