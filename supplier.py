import pika
import sys
import time
from itertools import batched

import shared


GEAR_TIME_COST = dict()
SUPPLIER_NAME = ""


def gear_callback(ch, method, properties, body):
    msg = body.decode("utf-8")
    print(msg)
    team_name, _, gear_name = method.routing_key.split(".")
    print(method.routing_key)
    tc = GEAR_TIME_COST[shared.Gear.from_string(gear_name)]

    print(f"Starting producing <{gear_name}> for team <{team_name}>, it will take {tc}s")

    time.sleep(tc)

    print(f"Produced <{gear_name}> for team <{team_name}>")
    ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_publish(exchange=shared.EXCHANGE_NAME, routing_key=shared.get_result_routing_key(team_name, SUPPLIER_NAME), body=f"produced {gear_name}".encode("utf-8"))




if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Not enough arguments")
        exit(1)
    
    _, name, *gear_info = sys.argv
    SUPPLIER_NAME = name
    for gear, time_cost in batched(gear_info, 2):
        try:
            gear_e = shared.Gear.from_string(gear)
            tc = int(time_cost)
            GEAR_TIME_COST[gear_e] = tc
        except ValueError as e:
            print(e)
            print(f"skipping {gear} : {time_cost}")
    
    print(f"Supplier [{name}]'s gear production time costs:")
    for k,v in GEAR_TIME_COST.items():
        print(f"{k}: {v}s")
    
    print()

    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    channel.exchange_declare(exchange=shared.EXCHANGE_NAME, exchange_type="topic")


    # Per Gear type queue
    for gear in GEAR_TIME_COST.keys():
        gear_q_name = shared.get_gear_q_name(gear)
        gear_routing_key = shared.get_gear_routing_key(gear)
        print(f"Queue <{gear_q_name}> with <{gear_routing_key}> routing key")

        q = channel.queue_declare(queue= gear_q_name, durable=True).method.queue
        channel.queue_bind(queue=q, exchange=shared.EXCHANGE_NAME, routing_key= gear_routing_key)
        channel.basic_consume(queue=q, on_message_callback=gear_callback, auto_ack=False)
    

    # Admin queue
    admin_q_name = shared.get_admin_q_name(SUPPLIER_NAME, shared.Mode.SUPPLIER)
    admin_routing_key_sup = shared.get_admin_routing_key(shared.Mode.SUPPLIER)
    admin_routing_key_all = shared.get_admin_routing_key(shared.Mode.ALL)
    print(f"Queue <{admin_q_name}> with <{admin_routing_key_sup}> and <{admin_routing_key_all}> routing keys")

    q = channel.queue_declare(queue= admin_q_name, durable=True).method.queue
    channel.queue_bind(queue=q, exchange=shared.EXCHANGE_NAME, routing_key= admin_routing_key_sup)
    channel.queue_bind(queue=q, exchange=shared.EXCHANGE_NAME, routing_key= admin_routing_key_all)
    channel.basic_consume(queue=q, on_message_callback=shared.admin_callback, auto_ack=False)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("")