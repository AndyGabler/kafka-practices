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
to hit a button to submit the schema to Schema Registry.

Once this is done, the application should produce a log like
```
2025-12-18T17:52:11.756-05:00  INFO 11184 --- [nfl-result-producer] [nio-8080-exec-7] i.g.a.n.r.p.schema.SchemaController      : Schema submitted with ID 1
```

At that point, Schema Registry should return a list of schemas if you navigate to
```
http://localhost:8081/subjects
```

This should return a single subject.
```json
[
  "nfl.game.result.v2"
]
```

Schema Registry will also return all Schemas at 
```
http://localhost:8081/schemas
```

But to view the specific one created, we can go to 
```
http://localhost:8081/schemas/ids/1
```
where "1" is the ID our application log produced.

This should give us the JSON schema we saw from the `/schemaInfo` endpoint.
```json
{
  "subject": "nfl.game.result.v2-value",
  "version": 1,
  "guid": "101b166e-288c-2181-85c6-4be717853b4e",
  "schemaType": "JSON",
  "schema": "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"$id\":\"https://example.com/product.schema.json\",\"title\":\"GameResult\",\"description\":\"Result of an NFL game.\",\"type\":\"object\",\"properties\":{\"homeTeam\":{\"description\":\"Team playing at home.\",\"$ref\":\"#/definitions/team\"},\"homeTeamScore\":{\"description\":\"Final score of the home team.\",\"type\":\"integer\"},\"visitingTeam\":{\"description\":\"Team visiting.\",\"$ref\":\"#/definitions/team\"},\"visitingTeamScore\":{\"description\":\"Final score of the visiting team.\",\"type\":\"integer\"}},\"required\":[\"homeTeam\",\"homeTeamScore\",\"visitingTeam\",\"visitingTeamScore\"],\"definitions\":{\"team\":{\"type\":\"string\",\"enum\":[\"49ers\",\"Bears\",\"Bengals\",\"Bills\",\"Broncos\",\"Browns\",\"Buccaneers\",\"Cardinals\",\"Chargers\",\"Chiefs\",\"Colts\",\"Commanders\",\"Cowboys\",\"Dolphins\",\"Eagles\",\"Falcons\",\"Giants\",\"Jaguars\",\"Jets\",\"Lions\",\"Packers\",\"Panthers\",\"Patriots\",\"Raiders\",\"Rams\",\"Ravens\",\"Saints\",\"Seahawks\",\"Steelers\",\"Texans\",\"Titans\",\"Vikings\"]}}}",
  "ts": 1765934924958,
  "deleted": false
}
```