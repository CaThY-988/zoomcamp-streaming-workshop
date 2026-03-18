import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import ride_deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='rides-console',
    value_deserializer=ride_deserializer
)

print(f"Listening to {topic_name}...")

count = 0
for message in consumer:
    ride = message.value
    #pickup_dt = datetime.fromtimestamp(ride.lpep_pickup_datetime / 1000)
    #dropoff_dt = datetime.fromtimestamp(ride.lpep_dropoff_datetime / 1000)
    
    print(
        f"Received: "
        f"PU={ride.PULocationID}, "
        f"DO={ride.DOLocationID}, "
        f"passengers={ride.passenger_count}, "
        f"distance={ride.trip_distance:.2f} mi, "
        f"tip=${ride.tip_amount:.2f}, "
        f"total=${ride.total_amount:.2f}, "
        f"total=${ride.lpep_pickup_datetime:.2f}",
        f"total=${ride.lpep_dropoff_datetime:.2f}"
        #f"pickup={pickup_dt}, "
        #f"dropoff={dropoff_dt}"
    )
    
    count += 1

consumer.close()