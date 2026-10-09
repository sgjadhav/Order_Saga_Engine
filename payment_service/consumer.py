import asyncio
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from payment_service.database import engine, Base, AsyncSessionLocal
from payment_service.models import Payment, ProcessedPaymentEvent

KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
INVENTORY_TOPIC = "inventory-events"
PAYMENT_TOPIC = "payment-events"

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def process_payment_event(event_data: dict, producer: AIOKafkaProducer):
    event_type = event_data.get("event_type")
    
    # Sirf InventoryReserved events ko process karna hai
    if event_type != "InventoryReserved":
        return

    order_id = event_data.get("order_id")
    quantity = event_data.get("quantity", 1)
    item_name = event_data.get("item_name")
    
    # Calculate amount: har keyboard 4500 ka hai
    amount = float(quantity) * 4500.0
    event_unique_id = f"payment_order_{order_id}"

    async with AsyncSessionLocal() as session:
        try:
            async with session.begin():
                # 1. ATOMIC DEDUPLICATION: Pehle insert karo
                session.add(ProcessedPaymentEvent(event_id=event_unique_id))
                await session.flush()

                # 2. Payment evaluation & Record creation
                if amount <= 10000.0:
                    payment_status = "SUCCESS"
                    out_event = {
                        "event_type": "PaymentCompleted",
                        "order_id": order_id,
                        "amount": amount,
                        "status": "PAID"
                    }
                else:
                    payment_status = "FAILED"
                    out_event = {
                        "event_type": "PaymentFailed",
                        "order_id": order_id,
                        "item_name": item_name,
                        "quantity": quantity,
                        "reason": "PAYMENT_LIMIT_EXCEEDED"
                    }

                new_payment = Payment(order_id=order_id, amount=amount, status=payment_status)
                session.add(new_payment)

            # 3. Publish event only after successful DB commit
            if payment_status == "SUCCESS":
                print(f"💳 Payment SUCCESS of ₹{amount} for Order #{order_id}!")
            else:
                print(f"❌ Payment FAILED of ₹{amount} for Order #{order_id} (Limit exceeded)!")

            await producer.send_and_wait(PAYMENT_TOPIC, json.dumps(out_event).encode('utf-8'))

        except IntegrityError:
            # Duplicate event caught atomically by Primary Key constraint
            await session.rollback()
            print(f"⚠️ [Idempotent Guard] Payment event '{event_unique_id}' already processed. Safely skipped.")

async def run_payment_consumer():
    await init_db()

    consumer = AIOKafkaConsumer(
        INVENTORY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="payment_service_group",
        auto_offset_reset="earliest"
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    await consumer.start()
    await producer.start()
    print("💳 Payment Consumer is listening for inventory events on Redpanda/Kafka...")

    try:
        async for msg in consumer:
            event_payload = json.loads(msg.value.decode('utf-8'))
            print(f"📥 [Payment] Received event: {event_payload}")
            await process_payment_event(event_payload, producer)
    except Exception as e:
        print(f"❌ Error in payment consumer: {e}")
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(run_payment_consumer())