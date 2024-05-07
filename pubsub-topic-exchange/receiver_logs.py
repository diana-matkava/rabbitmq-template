# To receive all the logs run:
# python receive_logs_topic.py "#"
#
# To receive all logs from the facility "kern":
# python receive_logs_topic.py "kern.*"
#
# Or if you want to hear only about "critical" logs:
# python receive_logs_topic.py "*.critical"
#
# You can create multiple bindings:
# python receive_logs_topic.py "kern.*" "*.critical"
#
# And to emit a log with a routing key "kern.critical" type:
# python emit_log_topic.py "kern.critical" "A critical kernel error"


import sys

import pika


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='0.0.0.0', port=5672))
    channel = connection.channel()
    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

    result = channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    binding_keys = sys.argv[1:]
    for key in binding_keys:
        channel.queue_bind(
            exchange='topic_logs',
            queue=queue_name,  # broadcasts all the messages by routing key
            routing_key=key,  # routing key is the severity of the log message
        )

    def logs(ch, method, properties, body):
        print("Not critical logs: %r" % body)

    channel.basic_consume(
        queue=queue_name, on_message_callback=logs, auto_ack=True)

    channel.start_consuming()


if __name__ == '__main__':
    main()



