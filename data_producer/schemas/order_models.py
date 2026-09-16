from datetime import datetime
from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator


class CorruptedEventError(Exception):
    """Raised when an incoming event fails strict schema enforcement."""
    pass


class OrderEventModel(BaseModel):
    order_id: str = Field(..., description="Unique UUID identifier for the order")
    user_id: str = Field(..., pattern=r"^USR_\d+$", description="User ID with USR_ prefix")
    rider_id: Optional[str] = Field(None, pattern=r"^RIDER_\d+$", description="Rider ID with RIDER_ prefix")
    city: Literal['Bengaluru', 'Mumbai', 'Delhi-NCR', 'Hyderabad', 'Chennai']
    category: Literal['Groceries', 'Dairy & Eggs', 'Snacks', 'Beverages', 'Personal Care']
    amount: float = Field(..., gt=0, description="Order amount in INR, must be strictly positive")
    payment_mode: Literal['UPI', 'Credit Card', 'NetBanking', 'COD']
    status: Literal['SUCCESS', 'FAILED', 'PENDING']
    prep_time_minutes: int = Field(..., ge=0, description="Minutes taken to prepare/pack order")
    transit_time_minutes: int = Field(..., ge=0, description="Minutes taken for rider transit")
    total_delivery_minutes: int = Field(..., ge=0, description="Total fulfillment time")
    is_sla_breached: bool = Field(..., description="Flag indicating if delivery exceeded target SLA")
    timestamp: datetime = Field(..., description="ISO 8601 UTC timestamp")
    schema_version: Optional[str] = "v1.2"

    @field_validator('amount')
    @classmethod
    def round_amount(cls, value: float) -> float:
        return round(value, 2)