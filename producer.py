"""Генератор IoT-событий -> Kafka topic raw-events.
Намеренно создаёт: дубли, опоздавшие события, нарушение порядка, битые сообщения.
Ведёт счётчики, чтобы потом сверить с Iceberg (без потерь и задвоений).
"""
import json, os, random, signal, time, uuid
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "raw-events")
RATE = float(os.getenv("EVENTS_PER_SEC", "100"))
TOTAL = int(os.getenv("TOTAL_EVENTS", "0"))        # 0 = бесконечно

p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": True,                      # защита от дублей при ретраях продюсера
    "linger.ms": 20,
})

stats = {"unique": 0, "duplicates": 0, "late": 0, "garbage": 0}
running = True
signal.signal(signal.SIGINT, lambda *_: globals().update(running=False))

def on_delivery(err, msg):
    if err:
        print("DELIVERY FAILED:", err)

def make_event():
    r = random.random()
    if r < 0.85:   delay = 0                          # вовремя
    elif r < 0.99: delay = random.randint(60, 1800)   # 1-30 минут
    else:          delay = random.randint(7200, 20000)  # несколько часов
    if delay: stats["late"] += 1
    ts = datetime.now(timezone.utc) - timedelta(seconds=delay)
    return {
        "event_id": str(uuid.uuid4()),
        "device_id": f"dev-{random.randint(1, 100)}",
        "event_ts": ts.isoformat(),
        "value": round(random.gauss(25, 5), 2),
    }

def send(raw: bytes, key: str):
    p.produce(TOPIC, value=raw, key=key, on_delivery=on_delivery)
    p.poll(0)

sent = 0
while running and (TOTAL == 0 or stats["unique"] < TOTAL):
    ev = make_event()
    raw = json.dumps(ev).encode()
    send(raw, ev["device_id"])
    stats["unique"] += 1

    if random.random() < 0.05:                        # дубль (retry устройства)
        send(raw, ev["device_id"])
        stats["duplicates"] += 1
    if random.random() < 0.005:                       # битое сообщение
        send(b"{not valid json", "garbage")
        stats["garbage"] += 1

    sent += 1
    if sent % 1000 == 0:
        print(stats)
    time.sleep(1.0 / RATE)

p.flush()
print("FINAL:", stats)    # unique должно совпасть с count(DISTINCT event_id) в Silver
