# Kafka Connect and Streams Exercise

Goal is to demonstrate using Kafka Streams and Kafka Connect.

There will be a database of NFL scores. These will have a foreign key to an NFL game.

Goal is to have a few different topics.
 * Topic for the football games table
 * Topic for the scores table
 * Kafka Connect will create topics from the games and scores tables
 * We will have an application that will POST, GET and DELETE from the database
 * We will have a Streams application to consume the topics and create game results from them

## Architecture

![Architecture](/docs/Architecture%20Diagram.drawio.png)

## Local Setup/Demo

This serves as documentation for what this looks like locally.

### Prerequisites

 * Must have Docker installed
 * Must have Python installed with the following packages (3.9.7 was used for demo):
   * `json`
   * `random`
   * `requests`
 * Recommended to have PG Admin 4 to connect to PostgresQL database, other method to connect can be used
 * Not required but demo is on Windows machine with Powershell
 * Not required but demo for run configs is on IntelliJ IDEA

### Setup

#### 1. Deploy Postgres, Kafka, Schema Registry and Kafka Connect

To deploy all necessary Kafka resources, open a command prompt in the `deploy-kafka-docker` directory. Run the command
```
docker-compose up -d
```

This should create a new Docker deployment.
![Proper Docker Setup](/docs/demo/01-1%20Docker%20Setup.PNG)

Make sure to reset your terminal/command prompt to the root directory for the rest of the demo.

#### 2. Running Liquibase Changes

To create new tables, run the Liquibase app in this project.
```
gradlew :nfl-score-liquibase:bootRun
```

After this is done, open PG Admin 4. The connection details are as follows:
TODO

The following queries can be run to directly see the tables created.
```sql
--Query for football games
SELECT * FROM football_game;
```
```sql
--Query for scores
SELECT * FROM game_score;
```
At this time you should see zero rows.

This step should have also enabled CDC the tables.

#### 3. Create Kafka Topics

For the Kafka topics that are internal to the streams application, we will need to manually create them.

Shell into the Kafka docker container using Windows PowerShell.
```ps
docker exec -it kafka bash
```

Run this command.
```sh
kafka-topics --create \
  --topic nflscoredatabase.public.football_game.rekey \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

kafka-topics --create \
  --topic nflscoredatabase.public.game_score.rekey \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

kafka-topics --create \
  --topic nflscoredatabase.public.score_and_game_join \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

kafka-topics --create \
  --topic nflscoredatabase.sink.game_result \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

#### 4. Create Sample Data

Run the Rest API that is sitting over the database.
```
gradlew :nfl-score-database-rest-api:bootRun
```

Next, assuming `py` is your Python command, run the following command.
```ps
py create_games_and_scores.py
```

This will create mock data to put into the database by creating a series of POST requests to the Rest API.

To verify it worked, you can re-run the queries from earlier.

Or you can use the Rest API by using any of the following URLs:

http://localhost:8084/game?page=0&sort=matchDate,desc (games, pageable)
![Games](/docs/demo/04-1%20games.PNG)

http://localhost:8084/score?page=0 (scores, pageable)
![Scores](/docs/demo/04-2%20scores.PNG)

http://localhost:8084/game/8 (game by ID)
![Game by ID](/docs/demo/04-3%20game%20by%20id.PNG)

http://localhost:8084/score/292 (score by ID)
![Score by ID](/docs/demo/04-4%20score%20by%20id.PNG)

#### 5. Create Kafka Connectors

There are two shell scripts in the `nfl-score-database-connect` directory. Run both. These create the source and
sink connectors.

#### 6. Run Streams Application

Finally, run the application that streams data between Kafka topics and populates the sink topics with the final game
results.
```
gradlew :nfl-score-streams:bootRun
```

After a while, this query should return results.
```sql
SELECT * 
FROM nflscoredatabase_sink_game_result
ORDER BY id desc;
```
![Game Results](/docs/demo/06-1%20results%20tables%20populated.PNG)

This query can be used to spot-check an individual game result against SQL. In this case, I spot-checked Game 76,
the Buccaneers vs Titans and game up with the same score as my Kafka Streams app (13-31).
```sql
SELECT
	game.id,
	game.home_team,
	game.visiting_team,
	home_score.home_score_total,
	visiting_score.visiting_score_total
FROM football_game game
CROSS JOIN LATERAL (
	SELECT
		COALESCE(SUM(p.Points), 0) AS "home_score_total"
	FROM (
		SELECT
			CASE 
				WHEN sc.snap_type = 'PASSING' THEN 6
				WHEN sc.snap_type = 'EXTRA POINT KICK' THEN 1
				WHEN sc.snap_type = 'RUSHING' THEN 6
				WHEN sc.snap_type = 'FIELD GOAL' THEN 3
				WHEN sc.snap_type = '2-POINT CONVERSION' THEN 2
			END AS "points"
		FROM game_score sc
		WHERE sc.football_game_id = game.ID AND sc.team = game.home_team
	) p
) home_score
CROSS JOIN LATERAL (
	SELECT
		COALESCE(SUM(p.Points), 0) AS "visiting_score_total"
	FROM (
		SELECT
			CASE 
				WHEN sc.snap_type = 'PASSING' THEN 6
				WHEN sc.snap_type = 'EXTRA POINT KICK' THEN 1
				WHEN sc.snap_type = 'RUSHING' THEN 6
				WHEN sc.snap_type = 'FIELD GOAL' THEN 3
				WHEN sc.snap_type = '2-POINT CONVERSION' THEN 2
			END AS "points"
		FROM game_score sc
		WHERE sc.football_game_id = game.ID AND sc.team = game.visiting_team
	) p
) visiting_score
WHERE game.id = 76
```
![Results Verified](/docs/demo/06-2%20results%20verified.PNG)

## Helpful Links

 * **Sink Connector Config**: http://localhost:8083/connectors/sink-connector/config
 * **Sink Connector Status**: http://localhost:8083/connectors/sink-connector/status
 * **Source Connector Config**: http://localhost:8083/connectors/postgres-connector/config
 * **Source Connector Status**: http://localhost:8083/connectors/postgres-connector/status

## Topic Manifest

| Topic                                         | Purpose                                                                                                    |
|-----------------------------------------------|------------------------------------------------------------------------------------------------------------|
| `nflscoredatabase.public.football_game`       | Kafka Connect topic that has Debezium messages from the `football_game` Postgres table.                    |
| `nflscoredatabase.public.game_score`          | Kafka Connect topic that has Debezium messages from the `game_score` Postgres table.                       |
| `nflscoredatabase.public.football_game.rekey` | Downstream topic of `nflscoredatabase.public.football_game` except with new key (unpacks Debezium struct). |
| `nflscoredatabase.public.game_score.rekey`    | Downstream topic of `nflscoredatabase.public.game_score` except with new key (`football_game` ID)          |
| `nflscoredatabase.public.score_and_game_join` | Topic that joins games to all scores that happened in the game.                                            |
| `nflscoredatabase.sink.game_result`           | Kafka Connect Sink topic that is used to put the final score of games.                                     |

Note, other topics will end up being created like Liquibase changelog topics, as well as a feedback topic for the 
Sink connector's table being fed back to the source connector.

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
for topic in $(kafka-topics --bootstrap-server localhost:9092 --list | grep nflscoredatabase); do
   kafka-configs \
     --bootstrap-server localhost:9092 \
     --entity-type topics \
     --entity-name $topic \
     --alter \
     --add-config retention.ms=1
 done
```
