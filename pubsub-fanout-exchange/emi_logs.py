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
        channel.exchange_declare(exchange='logs', exchange_type='fanout')

        if args.do == "send_log":
            channel.basic_publish(
                exchange="logs",
                routing_key="",
                body=args.body,
            )
            print("Sending task")

