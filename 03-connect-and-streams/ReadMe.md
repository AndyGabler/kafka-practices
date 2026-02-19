# Kafka Connect and Streams Exercise

Goal is to demonstrate using Kafka Streams and Kafka Connect.

There will be a database of NFL scores. These will have a foreign key to an NFL game.

Goal is to have a few different topics.
 * Topic for the football games table
 * Topic for the scores table
 * Kafka Connect will create topics from the games and scores tables
 * We will have an application that will POST, GET and DELETE from the database
 * We will have a Streams application to post to topics for game results and analysis topic