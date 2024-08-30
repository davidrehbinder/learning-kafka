#!/usr/bin/env python3

import argparse
import sys

from datetime import datetime as d
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

# Get arguments
parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(dest='command')
topics = subparsers.add_parser('topics', help='''Topics to listen to, separated
                               by spaces.\n
                               Has the arguments \'-p\' for which partition
                               to listen to (only if only one topic is chosen)
                               and \'-f\' to only follow the latest messages
                               (optional). See \'consumer.py topics -h\' for
                               more detailed help.''')
topics.add_argument('topics', nargs='+')
topics.add_argument('-p', '--partition',
                    nargs='?',
                    const=None,
                    help='''Partition to listen to. This cannot be selected
                    if more than one topic has been chosen. (Optional.)''')
topics.add_argument('-f', '--follow',
                    action='store_true',
                    help='''Commit and set offset to \'latest\'. (Optional.)''')

parser.add_argument('-l', '--list',
                    action='store_true',
                    help='''Show list of topics the user is authorized
                    to view.''')

# Print error if no arguments
if len(sys.argv) == 1:
    parser.print_help(sys.stderr)
    exit(1)

# And parse the arguments.
args = parser.parse_args()

auto_commit = False
offset = 'earliest'

if ('follow' in args):
    if args.follow == True:
        auto_commit = True
        offset = 'latest'
    elif args.follow == False:
        auto_commit = False
        offset = 'earliest'

# Create KafkaConsumer instance
consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset=offset,
    enable_auto_commit=auto_commit,
    group_id=None,
    key_deserializer=lambda k: k.decode('utf-8') if k is not None else k,
    value_deserializer=lambda x: x.decode('utf-8')
)

if args.command == 'topics':
    topic_list = args.topics
    if len(topic_list) > 1:
        if args.partition != None:
            print('Partition selection can only be done with one selected topic.')
            exit(1)

def listen():
    # Subscribe to topics
    if len(topic_list) > 1:
        consumer.subscribe(topic_list)
    else:
        topic = topic_list[0]
        if args.partition != None:
            topicpartitions = [] 
            for partition in args.partition:
                topicpartitions.append(TopicPartition(topic, int(partition)))
            consumer.assign(topicpartitions)
        else:
            consumer.subscribe(topic)
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
    consumer.close

# Listen to messages
if args.command == 'topics':
    listen()

if args.list != False:
    list_topics()