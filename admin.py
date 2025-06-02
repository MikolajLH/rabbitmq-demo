import sys
import pika
import shared
import threading
import time


def input_loop(prompt):
    while True:
        try:
            msg = input(prompt)
            if msg == "quit":
                return
            yield msg
        except KeyboardInterrupt:
            return
        except Exception:
            return
        

def callback(ch, method, properties, body):
    msg = body.decode("utf-8")
    print()
    print(f"New message matched to routing key {method.routing_key}")
    print(f"Msg: {msg}")
    print()
    
    ch.basic_ack(delivery_tag=method.delivery_tag)
        
def start_receiving(ch):
    q_name = "admin-q"
    admin_routing_key = "*.*.*"

    print(f"Queue <{q_name}> with <{admin_routing_key}> routing key")

    q = ch.queue_declare(queue= q_name, durable=True).method.queue
    ch.queue_bind(queue=q, exchange=shared.EXCHANGE_NAME, routing_key= admin_routing_key)
    ch.basic_consume(queue=q, on_message_callback=callback, auto_ack=False)

    ch.start_consuming()
        
if __name__ == "__main__":
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.exchange_declare(exchange=shared.EXCHANGE_NAME, exchange_type="topic")

    threading.Thread(target=start_receiving, args=[channel], daemon=True).start()

    for msg in input_loop(f""):
        cmds = msg.split()
        if len(cmds) < 2:
            print(f"Invalid input: {msg}; format is <destination> <msg>")
            continue
        
        dests, *msg = cmds
        msg = " ".join(msg)
        try:
            dest = shared.Mode.from_string(dests)
        except ValueError as er:
            print(f"There is no destination with name: {dests}")
            continue
        
        rk = shared.get_admin_routing_key(dest)
        print(f"Sending message <{msg}> with <{rk}> routing key")
        print()
        channel.basic_publish(exchange=shared.EXCHANGE_NAME, routing_key=rk, body=msg.encode())
        