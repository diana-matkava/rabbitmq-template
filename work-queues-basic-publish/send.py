import pika
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--do', type=str)
parser.add_argument('--body', type=str, default="{'data': 'value'}")


if __name__ == '__main__':
    args = parser.parse_args()

    with pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    ) as connection:

        channel = connection.channel()

        if args.do == 'send_message':
            channel.basic_publish(
                exchange='',  # nameless exchange.
                routing_key='send_message',  # The routing key to bind on queue
                # mast point the workers to the existing known queue
                body=args.body  # params for job
            )
            print(" [x] Sent 'Hello World!'")

        else:
            channel.basic_publish(
                exchange='',
                routing_key='task-per',
                body=args.body,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                )
            )
            print("Send Task")




