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


def result_callback(ch, method, properties, body):
    msg = body.decode("utf-8")
    supplier_name, _, team_name = method.routing_key.split(".")
    print()
    print(f"New message matched to routing key {method.routing_key}")
    print(f"supplier <{supplier_name}> to team <{team_name}> sent: {msg}")
    print()

    ch.basic_ack(delivery_tag=method.delivery_tag)



def start_receiving(ch, team_name):
    # supplier
    q_name = shared.get_result_q_name(team_name)
    routing_key = shared.get_result_routing_key(team_name=team_name)

    print(f"Queue <{q_name}> with <{routing_key}> routing key")

    q = ch.queue_declare(queue= q_name, durable=True).method.queue
    ch.queue_bind(queue=q, exchange=shared.EXCHANGE_NAME, routing_key= routing_key)
    ch.basic_consume(queue=q, on_message_callback=result_callback, auto_ack=False)

    # admin
    admin_q_name = shared.get_admin_q_name(team_name, shared.Mode.TEAM)
    admin_routing_key_tem = shared.get_admin_routing_key(shared.Mode.TEAM)
    admin_routing_key_all = shared.get_admin_routing_key(shared.Mode.ALL)

    print(f"Queue <{admin_q_name}> with <{admin_routing_key_tem}> and <{admin_routing_key_all}> routing keys")

    q = ch.queue_declare(queue= admin_q_name, durable=True).method.queue
    ch.queue_bind(queue=q, exchange=shared.EXCHANGE_NAME, routing_key= admin_routing_key_tem)
    ch.queue_bind(queue=q, exchange=shared.EXCHANGE_NAME, routing_key= admin_routing_key_all)
    ch.basic_consume(queue=q, on_message_callback=shared.admin_callback, auto_ack=False)

    ch.start_consuming()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("wrong arguments")
        exit(1)

    team_name = sys.argv[1]


    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.exchange_declare(exchange=shared.EXCHANGE_NAME, exchange_type="topic")

    threading.Thread(target=start_receiving, args=[channel, team_name], daemon=True).start()

    for msgs in input_loop(f""):
        gears = []
        try:
            for msg in msgs.split():
                gear = shared.Gear.from_string(msg)
                gears += [gear]
        except ValueError:
            print(f"There is no gear with name: {msg}")
            continue
        
        for gear in gears:
            channel.basic_publish(exchange=shared.EXCHANGE_NAME, routing_key=shared.get_gear_routing_key(gear, team_name), body=msg.encode())