from pydantic import BaseModel, ConfigDict
from typing import Optional

from pydantic import BaseModel, Field


class MLModelIn(BaseModel):
    customer_id: int
    product_id: int
    order_id: int
    purchase_date: str
    items: float


class MLModel(BaseModel):
    customer_id: int
    product_id: int
    prediction: int
    message: Optional[str] = None
