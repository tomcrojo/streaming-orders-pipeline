"""Kafka producer generating realistic order events with Spanish market data."""

from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from src.producer.config import (
    CUSTOMER_POOL_SIZE,
    DEFAULT_DURATION,
    DEFAULT_RATE,
    PRODUCER_CONFIG,
    PRODUCT_CATALOG_SIZE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("order_producer")

# ---------------------------------------------------------------------------
# Spanish market reference data
# ---------------------------------------------------------------------------
SPANISH_CITIES = [
    ("Madrid", "Madrid", "28001"),
    ("Barcelona", "Barcelona", "08001"),
    ("Valencia", "Valencia", "46001"),
    ("Sevilla", "Sevilla", "41001"),
    ("Zaragoza", "Zaragoza", "50001"),
    ("Málaga", "Málaga", "29001"),
    ("Murcia", "Murcia", "30001"),
    ("Palma", "Illes Balears", "07001"),
    ("Las Palmas", "Las Palmas", "35001"),
    ("Bilbao", "Bizkaia", "48001"),
    ("Alicante", "Alicante", "03001"),
    ("Córdoba", "Córdoba", "14001"),
    ("Valladolid", "Valladolid", "47001"),
    ("Vigo", "Pontevedra", "36201"),
    ("Gijón", "Asturias", "33201"),
    ("Hospitalet", "Barcelona", "08901"),
    ("A Coruña", "A Coruña", "15001"),
    ("Vitoria", "Álava", "01001"),
    ("Granada", "Granada", "18001"),
    ("Elche", "Alicante", "03201"),
    ("Oviedo", "Asturias", "33001"),
    ("Badalona", "Barcelona", "08911"),
    ("Cartagena", "Murcia", "30201"),
    ("Terrassa", "Barcelona", "08221"),
    ("Jerez", "Cádiz", "11401"),
    ("Sabadell", "Barcelona", "08201"),
    ("Santa Cruz", "Santa Cruz de Tenerife", "38001"),
    ("Pamplona", "Navarra", "31001"),
    ("Almería", "Almería", "04001"),
    ("San Sebastián", "Gipuzkoa", "20001"),
]

SPANISH_FIRST_NAMES = [
    "Alejandro",
    "Sofía",
    "Pablo",
    "Lucía",
    "Miguel",
    "Martina",
    "Diego",
    "Paula",
    "Daniel",
    "Julia",
    "Adrián",
    "Carla",
    "Álvaro",
    "Sara",
    "Hugo",
    "Alba",
    "Javier",
    "Ana",
    "David",
    "Carmen",
    "Iker",
    "Marta",
    "Mario",
    "Elena",
    "Carlos",
    "Laura",
    "Marco",
    "Isabel",
    "Antonio",
    "Clara",
    "Francisco",
    "Nerea",
    "Manuel",
    "Irene",
    "Gabriel",
    "Noa",
    "Raúl",
    "Andrea",
    "Jorge",
    "Celia",
    "Roberto",
    "Teresa",
    "Fernando",
    "Eva",
    "Rafael",
    "Berta",
    "Sergio",
    "Lidia",
]

SPANISH_LAST_NAMES = [
    "García",
    "Rodríguez",
    "González",
    "Fernández",
    "López",
    "Martínez",
    "Sánchez",
    "Pérez",
    "Gómez",
    "Martín",
    "Jiménez",
    "Ruiz",
    "Hernández",
    "Díaz",
    "Moreno",
    "Muñoz",
    "Álvarez",
    "Romero",
    "Alonso",
    "Gutiérrez",
    "Navarro",
    "Torres",
    "Domínguez",
    "Ramos",
    "Gil",
    "Vázquez",
    "Serrano",
    "Ramos",
    "Castro",
    "Blanco",
    "Suárez",
    "Molina",
    "Morales",
    "Ortega",
    "Delgado",
    "Castillo",
    "Cortés",
    "Sanz",
    "Rubio",
    "Nieto",
    "Medina",
    "Garrido",
    "Santos",
    "Guerrero",
    "Prieto",
    "Cruz",
    "Méndez",
    "Herrero",
    "Marín",
    "Calvo",
]

PRODUCT_CATEGORIES = {
    "Electrónica": [
        ("Auriculares Bluetooth", 29.99, 89.99),
        ("Cargador USB-C", 14.99, 39.99),
        ("Ratón inalámbrico", 19.99, 49.99),
        ("Teclado mecánico", 49.99, 129.99),
        ("Webcam HD", 34.99, 79.99),
        ("Monitor 27 pulgadas", 199.99, 499.99),
        ("SSD 1TB", 59.99, 119.99),
        ("Memoria RAM 16GB", 44.99, 99.99),
        ("Altavoz portátil", 24.99, 79.99),
        ("Powerbank 20000mAh", 19.99, 49.99),
        ("Hub USB", 14.99, 34.99),
        ("Cable HDMI 2m", 7.99, 19.99),
        ("Soporte portátil", 14.99, 39.99),
        ("Cámara de acción", 79.99, 199.99),
        ("MicroSD 128GB", 14.99, 29.99),
    ],
    "Hogar": [
        ("Cafetera espresso", 79.99, 299.99),
        ("Aspirador sin cable", 89.99, 249.99),
        ("Freidora de aire", 49.99, 149.99),
        ("Batidora", 29.99, 79.99),
        ("Tostador", 19.99, 49.99),
        ("Plancha de vapor", 24.99, 69.99),
        ("Humificador", 19.99, 59.99),
        ("Ventilador de torre", 29.99, 89.99),
        ("Lámpara LED", 14.99, 39.99),
        ("Set de sartenes", 34.99, 99.99),
        ("Robot de cocina", 99.99, 499.99),
        ("Hervidor eléctrico", 14.99, 34.99),
        ("Secador de pelo", 19.99, 79.99),
        ("Plancha de ropa", 29.99, 89.99),
        ("Purificador de aire", 59.99, 199.99),
    ],
    "Ropa y Accesorios": [
        ("Camiseta algodón", 9.99, 24.99),
        ("Pantalón vaquero", 29.99, 59.99),
        ("Chaqueta invierno", 49.99, 129.99),
        ("Zapatillas running", 59.99, 129.99),
        ("Mochila urbana", 24.99, 59.99),
        ("Cinturón cuero", 14.99, 39.99),
        ("Gorra", 9.99, 19.99),
        ("Bufanda", 12.99, 29.99),
        ("Reloj deportivo", 39.99, 99.99),
        ("Gafas de sol", 19.99, 59.99),
        ("Sudadera", 24.99, 49.99),
        ("Calcetines pack 3", 7.99, 14.99),
        ("Camisa formal", 29.99, 59.99),
        ("Falda", 19.99, 44.99),
        ("Abrigo lana", 79.99, 199.99),
    ],
    "Deportes": [
        ("Balón fútbol", 14.99, 39.99),
        ("Esterilla yoga", 12.99, 34.99),
        ("Mancuernas 2kg", 14.99, 29.99),
        ("Bicleta estática", 149.99, 399.99),
        ("Cinta correr", 299.99, 999.99),
        ("Banda elástica", 9.99, 24.99),
        ("Botella deportiva", 7.99, 19.99),
        ("Rueda abdominal", 12.99, 24.99),
        ("Comba", 4.99, 14.99),
        ("Casco bicicleta", 24.99, 69.99),
        ("Guantes gimnasio", 9.99, 19.99),
        ("Pelota pilates", 12.99, 29.99),
        ("Pesa rusa", 19.99, 59.99),
        ("Botas trekking", 49.99, 129.99),
        ("Red tenis mesa", 9.99, 24.99),
    ],
    "Libros y Cultura": [
        ("Novela bestseller", 9.99, 19.99),
        ("Libro técnico", 24.99, 49.99),
        ("Manga", 7.99, 12.99),
        ("Cómic", 14.99, 24.99),
        ("Guía turística", 12.99, 24.99),
        ("Árte de fotografía", 19.99, 49.99),
        ("Cuaderno artista", 9.99, 19.99),
        ("Set rotuladores", 7.99, 24.99),
        ("Puzzle 1000 piezas", 12.99, 24.99),
        ("Juego mesa", 19.99, 49.99),
        ("Biografía", 14.99, 24.99),
        ("Enciclopedia", 29.99, 59.99),
        ("Diccionario", 19.99, 39.99),
        ("Atlas ilustrado", 24.99, 49.99),
        ("Poemario", 9.99, 16.99),
    ],
}

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bizum", "bank_transfer"]
PAYMENT_WEIGHTS = [35, 30, 20, 10, 5]


def generate_customer_pool(size: int = CUSTOMER_POOL_SIZE) -> list[dict[str, Any]]:
    """Generate a pool of simulated Spanish customers."""
    random.seed(42)
    customers = []
    for i in range(size):
        city, province, postal_code = random.choice(SPANISH_CITIES)
        customers.append(
            {
                "customer_id": f"CUST-{i:06d}",
                "first_name": random.choice(SPANISH_FIRST_NAMES),
                "last_name": random.choice(SPANISH_LAST_NAMES),
                "email": f"user{i}@email.es",
                "city": city,
                "province": province,
                "postal_code": postal_code,
                "country": "España",
            }
        )
    return customers


def generate_product_catalog(size: int = PRODUCT_CATALOG_SIZE) -> list[dict[str, Any]]:
    """Generate a product catalog from Spanish market categories."""
    random.seed(43)
    catalog = []
    pid = 0
    for category, items in PRODUCT_CATEGORIES.items():
        for name, price_min, price_max in items:
            price = round(random.uniform(price_min, price_max), 2)
            catalog.append(
                {
                    "product_id": f"PROD-{pid:05d}",
                    "product_name": name,
                    "category": category,
                    "base_price": price,
                }
            )
            pid += 1
    # Fill remaining slots with random variations
    while len(catalog) < size:
        base = random.choice(catalog)
        catalog.append(
            {
                "product_id": f"PROD-{pid:05d}",
                "product_name": f"{base['product_name']} Pro",
                "category": base["category"],
                "base_price": round(base["base_price"] * 1.3, 2),
            }
        )
        pid += 1
    return catalog[:size]


class OrderEventGenerator:
    """Generates realistic order event sequences."""

    def __init__(self) -> None:
        self.customers = generate_customer_pool()
        self.products = generate_product_catalog()
        self._active_orders: dict[str, dict[str, Any]] = {}

    def _make_address(self, customer: dict[str, Any]) -> dict[str, str]:
        street_num = random.randint(1, 200)
        streets = [
            "Calle Mayor",
            "Avenida de la Constitución",
            "Plaza España",
            "Calle Gran Vía",
            "Paseo de la Castellana",
            "Calle Serrano",
            "Calle Alcalá",
            "Calle Princesa",
            "Calle Fuencarral",
            "Avenida Diagonal",
            "Rambla Cataluña",
            "Paseo del Prado",
        ]
        return {
            "street": f"{random.choice(streets)}, {street_num}",
            "city": customer["city"],
            "province": customer["province"],
            "postal_code": customer["postal_code"],
            "country": "España",
        }

    def _pick_products(self) -> list[dict[str, Any]]:
        num_items = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5])[0]
        selected = random.sample(self.products, min(num_items, len(self.products)))
        items = []
        for product in selected:
            qty = random.choices([1, 2, 3], weights=[70, 20, 10])[0]
            items.append(
                {
                    "product_id": product["product_id"],
                    "product_name": product["product_name"],
                    "quantity": qty,
                    "unit_price": product["base_price"],
                    "subtotal": round(product["base_price"] * qty, 2),
                }
            )
        return items

    def _build_event(
        self,
        event_type: str,
        order_id: str,
        customer: dict[str, Any],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "order_id": order_id,
            "customer_id": customer["customer_id"],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }

    def generate_event(self) -> dict[str, Any]:
        """Generate the next order event.

        Produces a mix of new orders and lifecycle events for existing orders,
        simulating a realistic order flow.
        """
        # 40% chance to start a new order, 60% to progress an existing one
        if not self._active_orders or random.random() < 0.4:
            return self._generate_new_order()
        return self._progress_existing_order()

    def _generate_new_order(self) -> dict[str, Any]:
        customer = random.choice(self.customers)
        items = self._pick_products()
        total = round(sum(i["subtotal"] for i in items), 2)
        order_id = str(uuid.uuid4())
        payment = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]

        payload = {
            "items": items,
            "total_amount": total,
            "currency": "EUR",
            "shipping_address": self._make_address(customer),
            "payment_method": payment,
            "status": "created",
            "notes": "",
        }

        self._active_orders[order_id] = {
            "customer": customer,
            "items": items,
            "total": total,
            "status": "created",
            "payment": payment,
            "address": payload["shipping_address"],
        }

        return self._build_event("order_created", order_id, customer, payload)

    def _progress_existing_order(self) -> dict[str, Any]:
        order_id, order = random.choice(list(self._active_orders.items()))
        customer = order["customer"]
        current = order["status"]

        transitions = {
            "created": ["order_item_added", "order_confirmed", "order_cancelled"],
            "confirmed": ["order_shipped", "order_cancelled"],
            "shipped": ["order_delivered"],
        }

        if current not in transitions:
            # terminal state, clean up
            del self._active_orders[order_id]
            return self._generate_new_order()

        next_events = transitions[current]
        weights = {
            "order_item_added": 10,
            "order_confirmed": 40,
            "order_shipped": 50,
            "order_delivered": 60,
            "order_cancelled": 8,
        }
        event_weights = [weights.get(e, 1) for e in next_events]
        next_event = random.choices(next_events, weights=event_weights)[0]

        if next_event == "order_item_added":
            extra = self._pick_products()[:1]
            order["items"].extend(extra)
            order["total"] = round(
                order["total"] + sum(i["subtotal"] for i in extra), 2
            )
            payload = {
                "items": order["items"],
                "total_amount": order["total"],
                "currency": "EUR",
                "status": current,
                "added_items": extra,
            }
        elif next_event == "order_confirmed":
            order["status"] = "confirmed"
            payload = {
                "items": order["items"],
                "total_amount": order["total"],
                "currency": "EUR",
                "payment_method": order["payment"],
                "shipping_address": order["address"],
                "status": "confirmed",
            }
        elif next_event == "order_shipped":
            order["status"] = "shipped"
            tracking = f"ES{random.randint(100000000, 999999999)}"
            payload = {
                "items": order["items"],
                "total_amount": order["total"],
                "currency": "EUR",
                "status": "shipped",
                "tracking_number": tracking,
                "carrier": random.choice(["SEUR", "MRW", "Correos", "DHL", "GLS"]),
            }
        elif next_event == "order_delivered":
            order["status"] = "delivered"
            payload = {
                "items": order["items"],
                "total_amount": order["total"],
                "currency": "EUR",
                "status": "delivered",
            }
            del self._active_orders[order_id]
        elif next_event == "order_cancelled":
            order["status"] = "cancelled"
            reasons = [
                "Cambié de opinión",
                "Encontré mejor precio",
                "No lo necesito ya",
                "Error al realizar el pedido",
                "Demasiado tiempo de entrega",
            ]
            payload = {
                "items": order["items"],
                "total_amount": order["total"],
                "currency": "EUR",
                "status": "cancelled",
                "cancellation_reason": random.choice(reasons),
            }
            del self._active_orders[order_id]
        else:
            payload = {
                "items": order["items"],
                "total_amount": order["total"],
                "currency": "EUR",
                "status": current,
            }

        return self._build_event(next_event, order_id, customer, payload)


class OrderProducer:
    """Kafka producer for order events with dry-run support."""

    def __init__(
        self, bootstrap_servers: str, topic: str, dry_run: bool = False
    ) -> None:
        self.topic = topic
        self.dry_run = dry_run
        self._generator = OrderEventGenerator()
        self._producer = None
        self._sent_count = 0
        self._running = True

        if not dry_run:
            try:
                from confluent_kafka import Producer

                config = {**PRODUCER_CONFIG, "bootstrap.servers": bootstrap_servers}
                self._producer = Producer(config)
                logger.info("Kafka producer connected to %s", bootstrap_servers)
            except Exception as e:
                logger.warning(
                    "Cannot connect to Kafka: %s — falling back to dry-run", e
                )
                self.dry_run = True

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        if err:
            logger.error("Delivery failed: %s", err)
        else:
            self._sent_count += 1

    def send(self, event: dict[str, Any]) -> None:
        key = event["order_id"].encode("utf-8")
        value = json.dumps(event, ensure_ascii=False).encode("utf-8")

        if self.dry_run:
            print(json.dumps(event, ensure_ascii=False, indent=2))
            print("---")
            self._sent_count += 1
        else:
            self._producer.produce(
                self.topic,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )
            self._producer.poll(0)

    def flush(self) -> None:
        if self._producer:
            self._producer.flush(timeout=30)

    def run(self, rate: float = DEFAULT_RATE, duration: int = DEFAULT_DURATION) -> None:
        interval = 1.0 / rate if rate > 0 else 1.0
        start = time.monotonic()

        def _signal_handler(sig: Any, frame: Any) -> None:
            logger.info("Shutdown signal received")
            self._running = False

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        logger.info(
            "Starting producer: rate=%.1f events/s, duration=%ds, dry_run=%s",
            rate,
            duration,
            self.dry_run,
        )

        try:
            while self._running:
                if duration > 0 and (time.monotonic() - start) >= duration:
                    logger.info("Duration reached, stopping")
                    break

                event = self._generator.generate_event()
                self.send(event)

                if self._sent_count % 100 == 0:
                    logger.info(
                        "Sent %d events (%.1f/s active orders: %d)",
                        self._sent_count,
                        self._sent_count / max(time.monotonic() - start, 0.01),
                        len(self._generator._active_orders),
                    )

                time.sleep(max(0, interval))
        finally:
            self.flush()
            logger.info("Producer finished. Total events sent: %d", self._sent_count)


def main() -> None:
    parser = argparse.ArgumentParser(description="Order event Kafka producer")
    parser.add_argument(
        "--rate", type=float, default=DEFAULT_RATE, help="Events per second"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION,
        help="Duration in seconds (0=infinite)",
    )
    parser.add_argument(
        "--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers"
    )
    parser.add_argument("--topic", default="orders.created", help="Kafka topic")
    parser.add_argument(
        "--dry-run", action="store_true", help="Print to stdout instead of Kafka"
    )
    args = parser.parse_args()

    producer = OrderProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        dry_run=args.dry_run,
    )
    producer.run(rate=args.rate, duration=args.duration)


if __name__ == "__main__":
    main()
