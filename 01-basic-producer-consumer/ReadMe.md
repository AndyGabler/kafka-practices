# Kafka Learning

This is my repository for some experimenting I did while watching the Confluent Apache Kafka Tutorials (Kafka 101).
[Kafka 101](https://www.youtube.com/playlist?list=PLa7VYi0yPIH0KbnJQcMv5N9iW8HkZHztH)


## Setup

I setup my broker locally by pulling it down with Docker.
```
docker pull apache/kafka:4.1.0
```

I then started the container by running the command
```
docker run -p 9092:9092 apache/kafka:4.1.0
```