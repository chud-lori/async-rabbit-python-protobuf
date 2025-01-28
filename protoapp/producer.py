import pika
from message_pb2 import Message

def produce():
    # connect to rabbitmq
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='message_queue')

    # Declare a protobuf message
    message = Message()
    message.id = "12"
    message.content = "Hello there"

    # Serialize the message
    serialized_message = message.SerializeToString()

    # Publish the message
    channel.basic_publish(
        exchange='',
        routing_key='message_queue',
        body=serialized_message
    )
    print("Sent:", message)

    # Close connection
    connection.close()

if __name__ == "__main__":
    produce()


