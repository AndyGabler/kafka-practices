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

The producer application will automatically create a schema upon the creation of the first producer record. The Producer
is responsible for maintaining a schema.

After navigating to `http://localhost:8080/gameResult` and submitting a game result, a schema will appear on schema
registry at `http://localhost:8081/schemas`. This is the schema produced by this application.
```json
{
    "subject": "nfl.game.result.v2-value",
    "version": 1,
    "id": 3,
    "guid": "33ed4f5b-fa41-bfe5-3532-cb1e90b90b71",
    "schemaType": "JSON",
    "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Game Result\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"homeTeam\":{\"type\":\"string\",\"enum\":[\"Bears\",\"Bengals\",\"Bills\",\"Broncos\",\"Browns\",\"Buccaneers\",\"Cardinals\",\"Chargers\",\"Chiefs\",\"Colts\",\"Commanders\",\"Cowboys\",\"Dolphins\",\"Eagles\",\"Falcons\",\"FortyNiners\",\"Giants\",\"Jaguars\",\"Jets\",\"Lions\",\"Packers\",\"Panthers\",\"Patriots\",\"Raiders\",\"Rams\",\"Ravens\",\"Saints\",\"Seahawks\",\"Steelers\",\"Texans\",\"Titans\",\"Vikings\"]},\"homeTeamScore\":{\"type\":\"integer\"},\"visitingTeam\":{\"type\":\"string\",\"enum\":[\"Bears\",\"Bengals\",\"Bills\",\"Broncos\",\"Browns\",\"Buccaneers\",\"Cardinals\",\"Chargers\",\"Chiefs\",\"Colts\",\"Commanders\",\"Cowboys\",\"Dolphins\",\"Eagles\",\"Falcons\",\"FortyNiners\",\"Giants\",\"Jaguars\",\"Jets\",\"Lions\",\"Packers\",\"Panthers\",\"Patriots\",\"Raiders\",\"Rams\",\"Ravens\",\"Saints\",\"Seahawks\",\"Steelers\",\"Texans\",\"Titans\",\"Vikings\"]},\"visitingTeamScore\":{\"type\":\"integer\"}},\"required\":[\"homeTeam\",\"homeTeamScore\",\"visitingTeam\",\"visitingTeamScore\"]}"
  }
```