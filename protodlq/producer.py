import pika
from message_pb2 import Message

def setup_queues():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))

    channel = connection.channel()

    # Declare Dead Letter Exchange
    channel.exchange_declare(exchange='dlx', exchange_type='direct')

    # Declare Dead Letter Queue
    channel.queue_declare(queue='message_queue_dlq')
    channel.queue_bind(exchange='dlx', queue='message_queue_dlq', routing_key='dlx_key')

    # Declare main queue with dlx settings
    channel.queue_declare(
        queue='message_queue',
        arguments={
            'x-dead-letter-exchange': 'dlx', # link to the dlx
            'x-dead-letter-routing-key': 'dlx_key' # routing key for dlx
        }
    )

    print("Queues and exchanges are set up.")
    connection.close()

def produce():
    credentials = pika.PlainCredentials('user1', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    # Declare a Protobuf message
    message = Message()
    message.id = "12"
    message.content = "Hello, Main Queue!"

    # Serialize the message
    serialized_message = message.SerializeToString()

    channel.basic_publish(
        exchange='',
        routing_key='message_queue',
        body=serialized_message
    )

    print("Sent:", message)

    connection.close()

if __name__ == "__main__":
    setup_queues()
    produce()

