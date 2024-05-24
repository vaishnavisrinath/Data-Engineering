import os
import json
from datetime import datetime
from google.cloud import pubsub_v1, storage

# TODO: Update these with your project details
project_id = "dataengineering-420601"
subscription_id = "archivetest-sub"
bucket_name = "vsrinath-maintainence"

# Initialize Pub/Sub and Storage clients
subscriber = pubsub_v1.SubscriberClient()
storage_client = storage.Client()

# The `subscription_path` method creates a fully qualified identifier
subscription_path = subscriber.subscription_path(project_id, subscription_id)
bucket = storage_client.bucket(bucket_name)

# Generate the file name based on the current date
today = datetime.now().strftime("%Y-%m-%d")
file_name = f"received_messages_{today}.json"

# Open a file to temporarily store messages before uploading
local_file_path = f"/tmp/{file_name}"

# Dictionary to keep track of message counts per vehicle ID
message_counts = {}

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    json_message = json.loads(message.data.decode('utf-8'))
    vehicle_id = json_message.get("vehicle_id", "unknown")

    # Update the message count for this vehicle ID
    if vehicle_id not in message_counts:
        message_counts[vehicle_id] = 0
    message_counts[vehicle_id] += 1

    # Print the progress for this vehicle ID
    print(f"Received {message_counts[vehicle_id]} messages for vehicle ID {vehicle_id}")

    with open(local_file_path, 'a') as file:
        file.write(json.dumps(json_message) + '\n')
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print(f"Listening for messages on {subscription_path}..\n")

try:
    with subscriber:
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
except TimeoutError:
    streaming_pull_future.cancel()

# After receiving and processing the messages, upload the file to GCS
blob = bucket.blob(file_name)
blob.upload_from_filename(local_file_path)
print(f"Uploaded {file_name} to {bucket_name}")

# Optionally, delete the local file after upload
os.remove(local_file_path)
