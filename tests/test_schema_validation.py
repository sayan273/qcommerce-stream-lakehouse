import pytest
from datetime import datetime
from data_producer.schemas.order_models import OrderEventModel
from pydantic import ValidationError

def test_valid_order_event():
    valid_payload = {
        "order_id": "8482f33c-3ec2-4a0b-93ff-183e8b0a991e",
        "user_id": "USR_5432",
        "city": "Bengaluru",
        "category": "Groceries",
        "amount": 250.75,
        "payment_mode": "UPI",
        "status": "SUCCESS",
        "timestamp": datetime.utcnow().isoformat()
    }
    model = OrderEventModel(**valid_payload)
    assert model.amount == 250.75
    assert model.city == "Bengaluru"

def test_invalid_user_id_and_negative_amount():
    invalid_payload = {
        "order_id": "test-id",
        "user_id": "INVALID_USER",
        "city": "Bengaluru",
        "category": "Groceries",
        "amount": -50.0,
        "payment_mode": "UPI",
        "status": "SUCCESS",
        "timestamp": datetime.utcnow().isoformat()
    }
    with pytest.raises(ValidationError):
        OrderEventModel(**invalid_payload)