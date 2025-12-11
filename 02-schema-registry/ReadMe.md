# Schema Registry App

This sub-folder contains a =consumer app and a basic producer app. Goal of this was to demonstrate the
usage of Kafka Schema Registry to myself.

Both apps connect to a local Kafka instance running at `localhost:9092` and interact with a topic named 
`nfl.game-results`.

The Producer will take a Spring MVC form of NFL game wins. The Consumer will dump these wins in an
H2 in-memory database and simply serve them as a REST API.

## Setup

First, you will need to set up the Kafka environment on your local machine. To do this, open a command prompt in the
`docker` directory and run
```
docker-compose up -d
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

## Schema Setup

You can navigate to 
```
http://localhost:8080/schemaInfo
``` 
to hit a button to submit the
schema to Schema Registry.

The schema can be viewed at
```
```
