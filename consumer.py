#!/usr/bin/env python3

import argparse
import sys

from datetime import datetime as d
from kafka import KafkaConsumer

# Get arguments
parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group()
# These two are mutually exclusive
group.add_argument('-t', '--topics',
                    action='store_true',
                    help='''Show list of topics the user is authorized
                    to view.'''
                    )
group.add_argument('-l', '--listen',
                    help='''One or more topics to subscribe to,
                    separated by commas.''')

# Print error if no arguments
if len(sys.argv) == 1:
    parser.print_help(sys.stderr)
    exit(1)

# And parse the arguments.
args = parser.parse_args()

# Get list of topics if we're listening
if (args.listen != None):
    topics = args.listen.split(sep=',')

# Create KafkaConsumer instance
consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id=None,
    value_deserializer=lambda x: x.decode('utf-8')
)

def listen():
    # Subscribe to topics
    consumer.subscribe(topics=topics)
    try:
        for msg in consumer:
            print('Topic: {} | Partition: {} | Offset: {} | Key: {} | Value: {} | Timestamp: {}'
                .format(msg.topic,
                    msg.partition,
                    msg.offset,
                    msg.key,
                    msg.value,
                    d.fromtimestamp(round(msg.timestamp/1000)))
                )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def list_topics():
    topic_list = consumer.topics()
    print('Total number of topics:', len(topic_list))
    for topic in topic_list:
        partitions = list(consumer.partitions_for_topic(topic))
        print('* Topic:', topic)
        print('  * Partitions:', partitions)

# Listen to messages
if args.listen != None:
    listen()

if args.topics != False:
    list_topics()