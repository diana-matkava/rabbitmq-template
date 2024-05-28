# 1 task 1 worker

from time import sleep

import pika


def main():
    # parameters: An instance of pika.ConnectionParameters specifying the connection settings such as host, port, virtual host, credentials, etc.
    # on_open_callback: (Optional) A callback function to be called when the connection is successfully established.
    # on_open_error_callback: (Optional) A callback function to be called if an error occurs during connection establishment.
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))

    # new channel is created for each worker
    channel = connection.channel()

    # and declare 1 queue
    channel.queue_declare('send_message')

    # and declare 1 queue and persist the data (on the disk)
    channel.queue_declare('task-per', durable=True)

    def message_callback(ch, method, props, body):
        print(" [x] Received %r" % body)

    def task_callback(ch, method, props, body):
        print(" [x] Received %r" % body)
        sleep(2)
        print("Task completed %r" % body)

        # to delete task in persistence storage after it eventually executes
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # start consuming messages from a queue
    channel.basic_consume(queue='send_message', on_message_callback=message_callback)
    channel.basic_consume(queue='task-per', on_message_callback=task_callback)

    # set prefetch_count so then one worker does not take a bulk of all tasks.
    # basic quality of service fare dispatch mechanism
    channel.basic_qos(prefetch_count=1)  # Specifies a prefetch window in terms of whole messages
    # bool global_qos:    Should the QoS apply to all channels on the  connection.
    # int prefetch_size:  This field specifies the prefetch window size

    channel.start_consuming()


if __name__ == '__main__':
    main()


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    channel.queue_declare('send_message')