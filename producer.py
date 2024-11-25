#!/usr/bin/env python3

import argparse
import sys

from datetime import datetime as d
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin.new_partitions import NewPartitions
from kafka.admin.new_topic import NewTopic

# Get arguments
parser = argparse.ArgumentParser()
# These two are mutually exclusive
parser.add_argument('-t', '--topic',
                    help='''Select topic.'''
                    )
write = parser.add_argument_group('write', '''Writes to the topic.''')
write.add_argument('-w', '--write', nargs='?', type=int, const=0,
                   metavar='PARTITION',
                   help='''Write to the specified topic. The optional argument
                   is the partition ID to write to. (Default partition is 0.)''')
write.add_argument('-k', '--key', nargs=1, type=str,
                   help='''Explicitly set key for the messages so that they
                   won't be sent to random partitions. (Optional, default key
                   is None.)''')

partition = parser.add_argument_group('partition', '''Partitions the topic.''')
partition.add_argument('-p', '--partition', nargs=1, type=int,
                       metavar='PARTITIONS',
                       help='''How many partitions to split the topic into.''')


# Print error if no arguments
if len(sys.argv) == 1:
    parser.print_help(sys.stderr)
    exit(1)

# And parse the arguments.
args = parser.parse_args()

topic = args.topic

key = None

if (args.key != None):
    key = args.key[0]

# Get number of partitions for the topic (since this is used for
# both functionalities.)
consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
if (consumer.partitions_for_topic(topic) == None):
    admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
    new_topic = admin_client.create_topics({
        topic: NewTopic(name=topic, num_partitions=1)
    })
    admin_client.close()
else:
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
        if key != None:
            key = key.encode('utf-8')
        producer.send(topic, partition=partition, key=key, value=data.encode('utf-8'))
except KeyboardInterrupt:
    pass
finally:
    producer.close()