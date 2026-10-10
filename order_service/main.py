import json
from contextlib import asynccontextmanager
from typing import List
from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

try:
    from .database import engine, Base, get_db
    from .models import Order, OutboxMessage, OrderCreate, OrderUpdate, OrderResponse
except ImportError:
    from database import engine, Base, get_db
    from models import Order, OutboxMessage, OrderCreate, OrderUpdate, OrderResponse

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield

app = FastAPI(
    title="Order Service",
    description="Microservice for managing orders asynchronously with Transactional Outbox",
    version="1.0.0",
    lifespan=lifespan,
)

@app.get("/")
async def root():
    return {"service": "Order Service", "status": "running"}

@app.post("/orders", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(
    order_data: OrderCreate,
    db: AsyncSession = Depends(get_db)
):
    # Step A: Order Object
    new_order = Order(
        item_name=order_data.item_name,
        quantity=order_data.quantity,
        price=order_data.price,
        status="PENDING",
    )
    db.add(new_order)
    await db.flush()  # DB buffer se primary key id mil jaati hai

    # Step B: Outbox Event Create karo
    event_payload = {
        "order_id": new_order.id,
        "item_name": new_order.item_name,
        "quantity": new_order.quantity,
        "price": new_order.price,
        "total_amount": new_order.price * new_order.quantity,
        "status": new_order.status
    }
    outbox_entry = OutboxMessage(
        event_type="OrderCreated",
        payload=json.dumps(event_payload),
        processed="PENDING"
    )
    db.add(outbox_entry)

    # Step C: Single Atomic Commit (Order + Outbox entry)
    await db.commit()
    await db.refresh(new_order)
    return new_order

@app.get("/orders", response_model=List[OrderResponse])
async def list_orders(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Order).offset(skip).limit(limit))
    return result.scalars().all()

@app.get("/orders/{order_id}", response_model=OrderResponse)
async def get_order(order_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Order).where(Order.id == order_id))
    order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Order with id {order_id} not found")
    return order

@app.patch("/orders/{order_id}", response_model=OrderResponse)
async def update_order(order_id: int, order_data: OrderUpdate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Order).where(Order.id == order_id))
    order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Order with id {order_id} not found")
    
    update_data = order_data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(order, field, value)

    await db.commit()
    await db.refresh(order)
    return order

@app.delete("/orders/{order_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_order(order_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Order).where(Order.id == order_id))
    order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Order with id {order_id} not found")
    
    await db.delete(order)
    await db.commit()
    return None