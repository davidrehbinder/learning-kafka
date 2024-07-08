#!/usr/bin/env python3

import argparse
import sys

from datetime import datetime as d
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin.new_partitions import NewPartitions

# Get arguments
parser = argparse.ArgumentParser()
# These two are mutually exclusive
parser.add_argument('-t', '--topic',
                    help='''Select topic.'''
                    )
group = parser.add_mutually_exclusive_group()
group.add_argument('-p', '--partition', nargs=1, type=int,
                   help='''How many partitions to split the topic into.''')
group.add_argument('-w', '--write', nargs='?', type=int, const=0,
                   help='''To interactively write to the topic. Argument is
                   partition ID to write to (default is 0).''')

# Print error if no arguments
if len(sys.argv) == 1:
    parser.print_help(sys.stderr)
    exit(1)

# And parse the arguments.
args = parser.parse_args()

topic = args.topic

# Get number of partitions for the topic (since this is used for
# both functionalities.)
consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
partitions = list(consumer.partitions_for_topic(topic))
consumer.close()

# We check if we're going to partition the topic.
if args.partition != None:
    new_partitions = args.partition[0]
    # Also check if we try to split into too few partitions (eg equal or
    # lesser number than there alread are).
    if (len(partitions) >= new_partitions):
        print(('New number of partitions must be greater than current '
               'number of partitions.'))
        exit(1)
    else:
        client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
        rsp = client.create_partitions({
            topic: NewPartitions(new_partitions)
        })
        print(rsp)
        client.close()
    exit(0)

# Create KafkaProducer instance
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092']
)

partition = args.write

# A little ugly, but since the partition ids are 0-indexed, well...
if (partition >= len(partitions)):
    print('There aren\'t that many partitions in the topic.')
    exit(1)

try:
    print('sending to topic', str(topic) + ', partition', str(partition))
    while True:
        data = input()
        producer.send(topic, partition=partition, value=data.encode('utf-8'))
except KeyboardInterrupt:
    pass
finally:
    producer.close()