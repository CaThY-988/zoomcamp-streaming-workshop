import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import ride_deserializer

consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='check-topic',
    value_deserializer=ride_deserializer
)

seen = set()
count = 0
for message in consumer:
    ride = message.value
    key = (
        ride.lpep_pickup_datetime,
        ride.lpep_dropoff_datetime,
        ride.PULocationID,
        ride.DOLocationID,
        ride.passenger_count,
        round(ride.trip_distance, 2),
        round(ride.tip_amount, 2),
        round(ride.total_amount, 2)
    )
    seen.add(key)
    count += 1

    if count % 10000 == 0:
        print(f"Read {count} messages, {len(seen)} unique rides")

print(f"Total messages in topic: {count}")
print(f"Unique rides in topic: {len(seen)}")