import json
import os
import zlib
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1, storage
from datetime import datetime

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
compressed_file_name = f"received_messages_{today}.json.zlib"

# Open a file to temporarily store messages before uploading
local_file_path = f"/tmp/{file_name}"
compressed_file_path = f"/tmp/{compressed_file_name}"

# Dictionary to keep track of messages per vehicle ID
message_count = {}

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    json_message = json.loads(message.data.decode('utf-8'))
    vehicle_id = json_message.get('vehicle_id', 'unknown')

    # Update message count for the vehicle_id
    if vehicle_id not in message_count:
        message_count[vehicle_id] = 0
    message_count[vehicle_id] += 1

    # Print progress for the vehicle_id
    print(f"Received message for vehicle_id {vehicle_id}: {message_count[vehicle_id]} messages")

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

# Compress the data before uploading
with open(local_file_path, 'rb') as f_in:
    with open(compressed_file_path, 'wb') as f_out:
        f_out.write(zlib.compress(f_in.read()))

# Upload the compressed file to GCS
blob = bucket.blob(compressed_file_name)
blob.upload_from_filename(compressed_file_path)
print(f"Uploaded {compressed_file_name} to {bucket_name}")

# Calculate and print the size of the original and compressed files
original_size = os.path.getsize(local_file_path)
compressed_size = os.path.getsize(compressed_file_path)
print(f"Original size: {original_size} bytes")
print(f"Compressed size: {compressed_size} bytes")
print(f"Compression ratio: {compressed_size / original_size:.2f}")

# Optionally, delete the local files after upload
os.remove(local_file_path)
os.remove(compressed_file_path)
