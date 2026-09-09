from datetime import datetime
from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator


class OrderEventModel(BaseModel):
    order_id: str = Field(..., description="Unique UUID identifier for the order")
    user_id: str = Field(..., pattern=r"^USR_\d+$", description="User ID with USR_ prefix")
    city: Literal['Bengaluru', 'Mumbai', 'Delhi-NCR', 'Hyderabad', 'Chennai']
    category: Literal['Groceries', 'Dairy & Eggs', 'Snacks', 'Beverages', 'Personal Care']
    amount: float = Field(..., gt=0, description="Order amount in INR, must be strictly positive")
    payment_mode: Literal['UPI', 'Credit Card', 'NetBanking', 'COD']
    status: Literal['SUCCESS', 'FAILED', 'PENDING']
    timestamp: datetime = Field(..., description="ISO 8601 UTC timestamp")
    schema_version: Optional[str] = "v1.2"

    @field_validator('amount')
    @classmethod
    def round_amount(cls, value: float) -> float:
        return round(value, 2)


class CorruptedEventError(Exception):
    """Raised when an incoming event fails strict schema enforcement."""
    pass