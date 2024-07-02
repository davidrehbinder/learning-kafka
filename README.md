# ✉️ Kafka (on Docker)

A quick way to get a local Kafka instance up and running using Docker.

Related blog post: https://sahansera.dev/setting-up-kafka-locally-for-testing/

## Requirements

* Docker
* Python 3 (I wrote the scripts in Python 3.12.1, I'm uncertain how far back the compatibility goes but it should be a fair bit)
* The [`kafka-python-ng`](https://github.com/wbarnha/kafka-python-ng) Kafka client (due to [issues](https://github.com/dpkp/kafka-python/issues/2440) with the original `kafka-python` client)

## Usage

Run at the root:

```sh
docker-compose up -d
```

### Producer
```sh

docker exec --interactive --tty broker \
kafka-console-producer --bootstrap-server localhost:9092 \
                       --topic example-topic
```

Or `./producer.py -t your-topic-here -w` (optionally including partition ID after `-w`, the default is `0`). `producer.py` can also be used with the `-p` flag, which allows you to add partitions (input the number of partitions to split the topic into).

### Consumer

```sh
docker exec --interactive --tty broker \
kafka-console-consumer --bootstrap-server localhost:9092 \
                       --topic example-topic \
                       --from-beginning
```

Or `./consumer.py -t comma-separated-list-of-topics`, which listens to all partitions of the specified topics. `consumer.py` can also be used with the `-l` flag, which lists the topics the user is authorized to view and the partitions for each topic.

## Todo

Add more functionality to the Python scripts, like:
* Only listening to a specific partition.
* ACLs.
* Probably more?