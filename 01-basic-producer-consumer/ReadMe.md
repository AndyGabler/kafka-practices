# Basic Producer Consumer

This sub-folder contains a basic consumer app and a basic producer app. Goal of it was to demonstrate a few Kafka concepts 
to myself, namely:
 * Basic Connection with Producer
 * Basic Connection with Consumer
 * Partitioning, particularly with `null` message key
 * Initializing Consumer offsets

Both just connect to a local Kafka instance running at `localhost:9092` and interact with a topic named 
`my-kafka-topic.message`.

## Setup

I set up my broker locally by pulling it down with Docker.
```
docker pull apache/kafka:4.1.0
```

I then started the container by running the command
```
docker run -p 9092:9092 apache/kafka:4.1.0
```

## Running Programs

To run the producer, simply run
```
gradlew producer:run
```

To run the consumer, simply run
```
gradlew consumer:run
```

The producer will prompt for options to either close the program or send a message to the broker.

The consumer will prompt for the name of the consumer. After which, it will prompt for the Auto Reset Config setting.

Once that's all configured, it will poll for Kafka messages on the topic and log them.