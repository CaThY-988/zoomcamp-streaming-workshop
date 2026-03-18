import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from kafka import KafkaConsumer
from models import ride_deserializer

# Kafka config
server = 'localhost:9092'
topic_name = 'green-trips'

# PostgreSQL config
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password='postgres'
)
conn.autocommit = True
cur = conn.cursor()

# Safe Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',   # start from beginning if first run
    group_id='rides-to-postgres-v4',
    enable_auto_commit=True,
    value_deserializer=ride_deserializer
)

print(f"Listening to {topic_name} and writing to PostgreSQL safely...")

count = 0

for message in consumer:
    ride = message.value

    # Normalize timestamps (remove microseconds)
    pickup_dt = datetime.fromtimestamp(ride.lpep_pickup_datetime / 1000).replace(microsecond=0)
    dropoff_dt = datetime.fromtimestamp(ride.lpep_dropoff_datetime / 1000).replace(microsecond=0)

    # Round floats to avoid precision issues
    trip_distance = round(ride.trip_distance, 2)
    tip_amount = round(ride.tip_amount, 2)
    total_amount = round(ride.total_amount, 2)

    # Safe insert with ON CONFLICT DO NOTHING
    cur.execute("""
        INSERT INTO processed_events (
            lpep_pickup_datetime,
            lpep_dropoff_datetime,
            PULocationID,
            DOLocationID,
            passenger_count,
            trip_distance,
            tip_amount,
            total_amount,
            pickup_datetime
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        pickup_dt,
        dropoff_dt,
        ride.PULocationID,
        ride.DOLocationID,
        ride.passenger_count,
        trip_distance,
        tip_amount,
        total_amount,
        pickup_dt
    ))

    count += 1
    if count % 100 == 0:
        print(f"Inserted {count} rows (duplicates skipped)...")

consumer.close()
cur.close()
conn.close()