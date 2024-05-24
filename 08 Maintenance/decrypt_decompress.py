import zlib
from google.cloud import storage
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
import os
from datetime import datetime

# TODO: Update these with your project details
today = datetime.now().strftime("%Y-%m-%d")
bucket_name = "vsrinath-maintainence"
encrypted_file_name = "received_messages_{today}.json.zlib.enc"
decrypted_file_name = "decrypted_received_messages.json"
compressed_file_name = "received_messages_{today}.json.zlib"
local_encrypted_file_path = f"/tmp/{encrypted_file_name}"
local_decrypted_file_path = f"/tmp/{decrypted_file_name}"
local_compressed_file_path = f"/tmp/{compressed_file_name}"

# Initialize Storage client
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

# Download the encrypted file from GCS
blob = bucket.blob(encrypted_file_name)
blob.download_to_filename(local_encrypted_file_path)

# Load private key
with open('private.pem', 'rb') as f:
    private_key = RSA.import_key(f.read())

try:
    # Decrypt the file
    with open(local_encrypted_file_path, 'rb') as f_in:
        encrypted_data = f_in.read()
        cipher = PKCS1_OAEP.new(private_key)
        decrypted_data = cipher.decrypt(encrypted_data)
    
    # Write decrypted data to a temporary file
    with open(local_compressed_file_path, 'wb') as f_out:
        f_out.write(decrypted_data)
    
    # Decompress the file
    with open(local_compressed_file_path, 'rb') as f_in:
        decompressed_data = zlib.decompress(f_in.read())
    
    # Write decompressed data to the final output file
    with open(local_decrypted_file_path, 'wb') as f_out:
        f_out.write(decompressed_data)
    
    # Optionally, read and print the contents of the decrypted file
    with open(local_decrypted_file_path, 'r') as f:
        for line in f:
            print(line.strip())
    
except ValueError as e:
    print(f"Error: {e}")

# Optionally, delete the local files after processing
os.remove(local_encrypted_file_path)
os.remove(local_compressed_file_path)
os.remove(local_decrypted_file_path)
