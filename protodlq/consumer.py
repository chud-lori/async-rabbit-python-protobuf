import pika
from message_pb2 import Message

def consume():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    def callback(ch, method, properties, body):
        try:
            # deserialzied proto
            message = Message()
            message.ParseFromString(body)
            print(f"Received from main queue: ID={message.id}, Content={message.content}")
            import time
            time.sleep(2)
            print(f"ID: {message.id}, content: {message.content}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

            # simulate failed
            #raise Exception("Processing failed")

        except Exception as e:
            print(f"Error: {e}. Rejecting message.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue='message_queue', on_message_callback=callback)
    print("Waiting for messages in the main queue. To exit, press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    consume()


