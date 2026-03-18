# Now write a producer to send the green taxi data to the green-trips topic.

# Read the parquet file and keep only these columns:
# Convert each row to a dictionary and send it to the green-trips topic. 
# You'll need to handle the datetime columns - convert them to strings before serializing to JSON.
# Measure the time it takes to send the entire dataset and flush:

import dataclasses
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer
from models import Ride, ride_from_row


import pandas as pd
from kafka import KafkaProducer
import dataclasses
import time
import sys 

sys.path.insert(0, str(Path(__file__).parent.parent))

from models import Ride, ride_from_row

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']
server = 'localhost:9092'
topic_name = 'green-trips'


df = pd.read_parquet(url, columns=columns)

def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)

t0 = time.time()

for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
#    print(f"Sent: {ride}") -- including = 41 seconds 
#    time.sleep(0.01) -- including = 554 seconds

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')