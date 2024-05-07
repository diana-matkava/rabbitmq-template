import sys

import pika

if __name__ == "__main__":

    with pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    ) as connection:
        channel = connection.channel()
        # The direct exchange routing algorithm -
        # a message goes to the queues whose binding key exactly matches the routing key of the message.
        channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

        routing_key = sys.argv[1] if len(sys.argv) > 2 else 'anonymous.info'
        message = ' '.join(sys.argv[2:]) or 'Hello World!'
        channel.basic_publish(
            exchange='topic_logs', routing_key=routing_key, body=message)
        print(f" [x] Sent {routing_key}:{message}")



