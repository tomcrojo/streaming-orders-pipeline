"""Avro/JSON schemas for order events."""

from __future__ import annotations

EVENT_TYPES = [
    "order_created",
    "order_item_added",
    "order_confirmed",
    "order_shipped",
    "order_delivered",
    "order_cancelled",
]

EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        "event_id": {"type": "string", "format": "uuid"},
        "event_type": {"type": "string", "enum": EVENT_TYPES},
        "order_id": {"type": "string", "format": "uuid"},
        "customer_id": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "payload": {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "product_id": {"type": "string"},
                            "product_name": {"type": "string"},
                            "quantity": {"type": "integer", "minimum": 1},
                            "unit_price": {"type": "number", "minimum": 0},
                            "subtotal": {"type": "number", "minimum": 0},
                        },
                        "required": [
                            "product_id",
                            "product_name",
                            "quantity",
                            "unit_price",
                            "subtotal",
                        ],
                    },
                },
                "total_amount": {"type": "number", "minimum": 0},
                "currency": {"type": "string", "const": "EUR"},
                "shipping_address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"},
                        "province": {"type": "string"},
                        "postal_code": {"type": "string"},
                        "country": {"type": "string"},
                    },
                },
                "payment_method": {
                    "type": "string",
                    "enum": [
                        "credit_card",
                        "debit_card",
                        "paypal",
                        "bizum",
                        "bank_transfer",
                    ],
                },
                "status": {"type": "string"},
                "notes": {"type": "string"},
            },
        },
    },
    "required": [
        "event_id",
        "event_type",
        "order_id",
        "customer_id",
        "timestamp",
        "payload",
    ],
}
