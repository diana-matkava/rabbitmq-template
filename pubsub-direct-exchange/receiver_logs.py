import pika


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='0.0.0.0', port=5672))
    channel = connection.channel()
    channel.exchange_declare(
        exchange='direct_logs',
        exchange_type='direct',  # broadcasts all the messages by routing key
    )

    errors = channel.queue_declare(queue='', exclusive=True)
    errors_queue_name = errors.method.queue

    other = channel.queue_declare(queue='', exclusive=True)
    other_queue_name = other.method.queue
    # we need a fresh, empty queue, because we dont want to send historical logs
    # if queue name is "" Rabbit will generate random name.
    # exclusive=True will delete the queue after connection closes
    for severity in ['info', 'warning']:
        channel.queue_bind(
            exchange='direct_logs',
            queue=other_queue_name,  # broadcasts all the messages by routing key
            routing_key=severity,  # routing key is the severity of the log message
        )

    channel.queue_bind(
        exchange='direct_logs',
        queue=errors_queue_name,  # broadcasts all the messages by routing key
        routing_key="error",  # routing key is the severity of the log message
    )

    def other_logs(ch, method, properties, body):
        print("Not critical logs: %r" % body)

    def error_logs(ch, method, properties, body):
        print("Error logs: %r" % body)

    channel.basic_consume(queue=other_queue_name, on_message_callback=other_logs, auto_ack=True)
    channel.basic_consume(queue=errors_queue_name, on_message_callback=error_logs, auto_ack=True)

    channel.start_consuming()


if __name__ == '__main__':
    main()

