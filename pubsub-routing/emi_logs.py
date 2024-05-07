import pika
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--do', type=str)
parser.add_argument('--body', type=str, default="{'data': 'value'}")


if __name__ == "__main__":
    args = parser.parse_args()

    with pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    ) as connection:
        channel = connection.channel()
        # The direct exchange routing algorithm -
        # a message goes to the queues whose binding key exactly matches the routing key of the message.
        channel.exchange_declare(exchange='logs', exchange_type='direct')

        if args.do == "send_log":
            channel.basic_publish(
                exchange="logs",
                routing_key="",
                body=args.body,
            )
            print("Sending task")

