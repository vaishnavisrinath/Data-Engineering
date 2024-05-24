import json
import os
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from datetime import datetime

# TODO: Update these with your project details
project_id = "dataengineering-420601"
subscription_id = "archivetest-sub"
# Number of seconds the subscriber should listen for messages

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # Decode the message data and print it (optional)
    json_message = json.loads(message.data.decode('utf-8'))
    print(json_message)  # Optional: Print the message to see the content
    # Acknowledge the message so it's removed from the subscription
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
