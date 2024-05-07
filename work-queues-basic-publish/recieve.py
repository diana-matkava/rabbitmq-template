# 1 task 1 worker

from time import sleep

import pika


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    channel.queue_declare('send_message')
    # RabbitMQ will *save data* *on disk* for persistence
    channel.queue_declare('task-per', durable=True)

    def message_callback(ch, method, props, body):
        print(" [x] Received %r" % body)

    channel.basic_consume(queue='send_message', on_message_callback=message_callback)

    def task_callback(ch, method, props, body):
        print(" [x] Received %r" % body)
        sleep(2)
        print("Task completed %r" % body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # so then one worker does not take a bulk of all tasks.
    channel.basic_qos(prefetch_count=1)

    # to delete task in persistence storage after it eventually executes
    channel.basic_consume(queue='task-per', on_message_callback=task_callback)

    channel.start_consuming()


if __name__ == '__main__':
    main()

