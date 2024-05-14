from time import sleep

import pika


connection = pika.BlockingConnection(pika.ConnectionParameters(host="127.0.0.1"))
channel = connection.channel()

channel.queue_declare(queue='rpc_queue')


def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


def callback(ch, method, props, body):
    v = int(body)
    res = fib(v)
    print(f"Got message: {body}")
    print(f"Sending...")
    sleep(2)
    ch.basic_publish(
        # exchange: The exchange to publish to
        # routing_key: The routing key to bind on
        # body: The message body
        # pika.spec.BasicProperties properties: Basic.properties
        # mandatory: The mandatory flag
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(
            correlation_id=props.correlation_id,
        ),
        body=str(res)
    )
    print(f"Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)  # acknowledge task in case of outages


channel.basic_qos(prefetch_count=1)  # spread the load equally over multiple servers
channel.basic_consume(queue='rpc_queue', on_message_callback=callback)  # consume messages from queue

print(" [x] Awaiting RPC requests")
channel.start_consuming()
