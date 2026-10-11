import asyncio
import json
import os
from aiokafka import AIOKafkaProducer
from sqlalchemy import select
from inventory_service.database import AsyncSessionLocal
from inventory_service.models import OutboxMessage

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092')
KAFKA_TOPIC = "inventory-events"


async def run_relay_worker():
    # 1. Initialize Async Kafka Producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    print("🚀 Inventory Outbox Relay Worker started and connected to Redpanda/Kafka!")

    try:
        while True:
            # 2. Create DB session and fetch un-processed events.
            # SKIP LOCKED: concurrent relay workers never pick the same row.
            async with AsyncSessionLocal() as session:
                query = (
                    select(OutboxMessage)
                    .where(OutboxMessage.processed == 'PENDING')
                    .with_for_update(skip_locked=True)
                    .limit(10)
                )
                result = await session.execute(query)
                pending_messages = result.scalars().all()

                for msg in pending_messages:
                    print(f"📦 Relaying Event ID: {msg.id} | Type: {msg.event_type}")

                    # 3. Publish to Kafka with order_id as the partition key so all
                    # events for one order stay ordered within a partition.
                    payload_dict = json.loads(msg.payload)
                    message_bytes = msg.payload.encode('utf-8')
                    await producer.send_and_wait(
                        KAFKA_TOPIC,
                        message_bytes,
                        key=str(payload_dict.get('order_id')).encode('utf-8')
                    )

                    # 4. Outbox table mein status update karo
                    msg.processed = 'PROCESSED'

                    # 5. Per-message commit: failure on message N must not cause
                    # messages 1..N-1 to be re-sent on restart
                    await session.commit()

                if pending_messages:
                    print(f"✅ Successfully processed {len(pending_messages)} event(s).")

            # 6. Har 2 second mein poll karo
            await asyncio.sleep(2)

    except Exception as e:
        print(f"❌ Inventory relay worker encountered an error: {e}")
    finally:
        await producer.stop()
        print("🛑 Inventory relay worker stopped.")


if __name__ == "__main__":
    asyncio.run(run_relay_worker())
