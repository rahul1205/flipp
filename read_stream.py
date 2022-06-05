#!/usr/bin/env python

"""Consumes stream for printing all messages to the console.
"""

import argparse
import json
import sys
import time
import socket
from dateutil.parser import parse
from confluent_kafka import Consumer, KafkaError, KafkaException

user_action = {}
flyer_avg = {}
flyer_avg_final = []


def msg_process(msg):
    # Print the current time and the message.
    time_start = time.strftime("%Y-%m-%d %H:%M:%S")
    message = json.loads(msg.value())
    print (message)
    timestamp, user_id, event, flyer_id, merchant_id = message["timestamp"], message["user_id"], message["event"], \
                                                       message["flyer_id"], message["merchant_id"]

    if user_id in user_action.keys():
        cur_flyer_id = list(user_action[user_id].keys())[0]
        cur_flyer_time = abs((parse(timestamp) - user_action[user_id][cur_flyer_id]).total_seconds())
        if cur_flyer_id in flyer_avg.keys():
            flyer_avg[cur_flyer_id].append(cur_flyer_time)
        else:
            flyer_avg[cur_flyer_id] = [cur_flyer_time]
        del user_action[user_id]

    if event == "flyer_open":
        if user_id not in user_action.keys():
            user_action[user_id] = {}
            user_action[user_id][flyer_id] = parse(timestamp)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')

    args = parser.parse_args()

    conf = {'bootstrap.servers': 'localhost:9092',
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'group.id': socket.gethostname()}

    consumer = Consumer(conf)

    running = True

    try:
        while running:
            consumer.subscribe([args.topic])

            msg = consumer.poll(1)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    sys.stderr.write('Topic unknown, creating %s topic\n' %
                                     (args.topic))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)

    except KeyboardInterrupt:
        pass

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
    for flyer, values in flyer_avg.items():
        avg_time = sum(values) / len(values)
        flyer_avg_final.append({"flyer_id":flyer, "avg_time":avg_time})
    print (flyer_avg_final)

if __name__ == "__main__":
    main()
