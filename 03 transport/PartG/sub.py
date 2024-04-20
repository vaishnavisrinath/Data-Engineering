  GNU nano 7.2                                   new_subscriber.py                                            
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import json
import sys

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        data_str = message.data.decode("utf-8")
        data = json.loads(data_str)
        print("Received message:", data)
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

def main(subscription_id):
    project_id = "dataengineering-420601"
    topic_id = "my-topic"
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = subscriber.topic_path(project_id, topic_id)
    subscription = subscriber.create_subscription(
        request={"name": subscription_path, "topic": topic_path}
    )
    print(f"Created subscription '{subscription.name}'.")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")
    with subscriber:
        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  
            streaming_pull_future.result()  
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python receiver.py <subscription_id>")
        sys.exit(1)
    
    subscription_id = sys.argv[1]
    main(subscription_id)