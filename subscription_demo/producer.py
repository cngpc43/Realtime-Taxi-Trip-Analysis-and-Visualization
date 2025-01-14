import pulsar
import pandas as pd
import time
import json

def main():
    # Initialize Pulsar client
    client = pulsar.Client('pulsar://127.0.0.1:6650')
    producer = client.create_producer('trip-topic')

    # Read the CSV dataset
    df = pd.read_csv('./NYC Taxi Trip Duration/train.csv', nrows=1000) 

    for index, row in df.iterrows():
        # Validate trip_duration (e.g., less than 2 hours)
        trip_duration_seconds = row['trip_duration']
        if trip_duration_seconds <= 0 or trip_duration_seconds > 7200:
            print(f"Skipping invalid trip_duration: {trip_duration_seconds}")
            continue  # Skip invalid entries

        # Convert trip_duration to minutes
        trip_duration_minutes = trip_duration_seconds / 60

        # Prepare trip data with cleaned duration
        trip_data = {
            'trip_duration': trip_duration_minutes,
            'timestamp': time.time(),
            'pickup_latitude': row.get('pickup_latitude'),
            'pickup_longitude': row.get('pickup_longitude'),
            'dropoff_latitude': row.get('dropoff_latitude'),
            'dropoff_longitude': row.get('dropoff_longitude')
        }

        # Convert to JSON string
        message = json.dumps(trip_data)
        producer.send(message.encode('utf-8'))
        print(f"Sent: {message}")
        time.sleep(0.1)  # Simulate real-time streaming

    producer.close()
    client.close()

if __name__ == "__main__":
    main()