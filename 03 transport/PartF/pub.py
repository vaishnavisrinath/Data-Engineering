from google.cloud import pubsub_v1
import json
import time

project_id = "dataengineering-420601"
topic_id = "my-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

with open("bcsample.json", "r") as file:
    data = json.load(file)

vehicle_3940_records = [record for record in data["vehicle_3940"]]
vehicle_3257_records = [record for record in data["vehicle_3257"]]

for record in vehicle_3940_records:
    data_str = json.dumps(record)
    data_bytes = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data_bytes)
    print(future.result())
    time.sleep(0.25)  # Sleep for 250 milliseconds

print(f"Published messages for vehicle 3940 to {topic_path}.")

for record in vehicle_3257_records:
    data_str = json.dumps(record)
    data_bytes = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data_bytes)
    print(future.result())
    time.sleep(0.25)  # Sleep for 250 milliseconds

print(f"Published messages for vehicle 3257 to {topic_path}.")
