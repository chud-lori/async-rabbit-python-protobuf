import pika
from message_pb2 import Message

def consume_dlq():
    credentials = pika.PlainCredentials('user1', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    def callback(ch, method, properties, body):
        # Deserialize the Protobuf message
        message = Message()
        message.ParseFromString(body)
        print(f"Received from DLQ: ID={message.id}, Content={message.content}")

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue='message_queue_dlq', on_message_callback=callback)
    print("Waiting for messages in the DLQ. To exit, press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    consume_dlq()

