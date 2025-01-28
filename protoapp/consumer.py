import pika
from message_pb2 import Message

def callback(ch, method, properties, body):
    # Deserialize the protobuf message
    message = Message()
    message.ParseFromString(body)

    # Process the message
    print("Received message:")
    print(f"ID: {message.id}, content: {message.content}")

def consume():
    # Connect to rabbitmq
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    # declare a queue
    channel.queue_declare(queue='message_queue')

    # Subscribe to the queue
    channel.basic_consume(queue='message_queue', on_message_callback=callback, auto_ack=True)

    print("Waiting for messages. To exit, press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    consume()

