# Kafka Connect and Streams Exercise

Goal is to demonstrate using Kafka Streams and Kafka Connect.

There will be a database of NFL scores. These will have a foreign key to an NFL game.

Goal is to have a few different topics.
 * Topic for the football games table
 * Topic for the scores table
 * Kafka Connect will create topics from the games and scores tables
 * We will have an application that will POST, GET and DELETE from the database
 * We will have a Streams application to post to topics for game results and analysis topic


http://localhost:8083/connector-plugins
http://localhost:8083/connectors
http://localhost:8083/connectors/postgres-connector


## Helpful Local Development Debug Commands

Use PowerShell to get into the Kafka broker.
```ps
docker exec -it kafka bash
```

Command to list topics.
```sh
kafka-topics --bootstrap-server localhost:29092 --list
```

Consume single message from a topic.
```sh
kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic nflscoredatabase.public.football_game \
  --from-beginning \
  --max-messages 1
```

Drop all messages on all topics (set back to -1 to keep retaining all)
```sh
for topic in $(kafka-topics.sh --bootstrap-server localhost:9092 --list | grep nflscoredatabase); do
   kafka-configs.sh \
     --bootstrap-server localhost:9092 \
     --entity-type topics \
     --entity-name $topic \
     --alter \
     --add-config retention.ms=1
 done
```