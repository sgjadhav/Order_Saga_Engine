import asyncio
import json
from aiokafka import AIOKafkaProducer
from sqlalchemy import select
from order_service.database import AsyncSessionLocal
from order_service.models import OutboxMessage

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "order-events"

async def run_relay_worker():
    # 1. Initialize Async Kafka Producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    print("🚀 Outbox Relay Worker started and connected to Redpanda/Kafka!")

    try:
        while True:
            # 2. Database Session create karke un-processed events dhoondo
            async with AsyncSessionLocal() as session:
                query = select(OutboxMessage).where(OutboxMessage.processed == "PENDING").limit(10)
                result = await session.execute(query)
                pending_messages = result.scalars().all()

                for msg in pending_messages:
                    print(f"📦 Relaying Event ID: {msg.id} | Type: {msg.event_type}")

                    # 3. Message ko Kafka topic par push karo
                    message_bytes = msg.payload.encode('utf-8')
                    await producer.send_and_wait(KAFKA_TOPIC, message_bytes)

                    # 4. Outbox table mein status update karo
                    msg.processed = "PROCESSED"
                
                # 5. DB changes commit karo
                if pending_messages:
                    await session.commit()
                    print(f"✅ Successfully processed {len(pending_messages)} event(s).")

            # 6. Har 2 second mein poll karo
            await asyncio.sleep(2)

    except Exception as e:
        print(f"❌ Worker encountered an error: {e}")
    finally:
        await producer.stop()
        print("🛑 Worker stopped.")

if __name__ == "__main__":
    asyncio.run(run_relay_worker())