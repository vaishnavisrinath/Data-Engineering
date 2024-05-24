import json
import os
import zlib
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1, storage
from datetime import datetime
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP, AES
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad

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
encrypted_file_name = f"received_messages_{today}.json.zlib.enc"

# Open a file to temporarily store messages before uploading
local_file_path = f"/tmp/{file_name}"
compressed_file_path = f"/tmp/{compressed_file_name}"
encrypted_file_path = f"/tmp/{encrypted_file_name}"

message_count = {}

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    json_message = json.loads(message.data.decode('utf-8'))
    vehicle_id = json_message.get("vehicle_id", "unknown")

    if vehicle_id not in message_count:
        message_count[vehicle_id] = 0
        # Update the message count for this vehicle ID
    message_count[vehicle_id] += 1
        # Print the progress for this vehicle ID
    print(f"Received {message_count[vehicle_id]} messages for vehicle ID {vehicle_id}")

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
if os.path.exists(local_file_path):
    with open(local_file_path, 'rb') as f_in:
        with open(compressed_file_path, 'wb') as f_out:
            f_out.write(zlib.compress(f_in.read()))

    # Load public key
    with open('public.pem', 'rb') as f:
        public_key = RSA.import_key(f.read())

    # Generate AES key
    aes_key = get_random_bytes(32)  # Using AES-256
    cipher_aes = AES.new(aes_key, AES.MODE_CBC)
    iv = cipher_aes.iv

    # Encrypt the compressed file data with AES
    with open(compressed_file_path, 'rb') as f_in:
        data = f_in.read()
        encrypted_data = cipher_aes.encrypt(pad(data, AES.block_size))

    # Encrypt the AES key with RSA
    cipher_rsa = PKCS1_OAEP.new(public_key)
    encrypted_aes_key = cipher_rsa.encrypt(aes_key)

    # Write the encrypted AES key and IV to the encrypted file
    with open(encrypted_file_path, 'wb') as f_out:
        f_out.write(iv + encrypted_aes_key + encrypted_data)

    # Upload the encrypted file to GCS
    blob = bucket.blob(encrypted_file_name)
    blob.upload_from_filename(encrypted_file_path)
    print(f"Uploaded {encrypted_file_name} to {bucket_name}")

    # Optionally, delete the local files after upload
   os.remove(local_file_path)
   os.remove(compressed_file_path)
   os.remove(encrypted_file_path)
else:
    print(f"File {local_file_path} does not exist.")
