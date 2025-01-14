import pulsar
import argparse
import json

def consume_messages(subscription_name, subscription_type):
    client = pulsar.Client('pulsar://localhost:6650')
    consumer = client.subscribe(
        'trip-topic',
        subscription_name=subscription_name,
        consumer_type=subscription_type
    )

    print(f"Consumer '{subscription_name}' started with '{subscription_type.name}' mode.")
    
    total_duration = 0
    trip_count = 0
    try:
        while True:
            msg = consumer.receive()
            data = json.loads(msg.data().decode('utf-8'))
            
            # Extract trip duration
            trip_duration = data.get('trip_duration', 0)
            total_duration += trip_duration
            trip_count += 1
            average_duration = total_duration / trip_count

            print(f"[{subscription_name}] Received trip_duration: {trip_duration} seconds | Average Duration: {average_duration:.2f} seconds")

            consumer.acknowledge(msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pulsar Consumer for Taxi Trip Data")
    parser.add_argument("--subscription", required=True, help="Subscription name")
    parser.add_argument("--type", choices=["Exclusive", "Shared", "Failover"], required=True, help="Subscription type")
    args = parser.parse_args()

    subscription_type = getattr(pulsar.ConsumerType, args.type)
    consume_messages(args.subscription, subscription_type)