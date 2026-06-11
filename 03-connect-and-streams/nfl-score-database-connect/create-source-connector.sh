curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
        "name": "postgres-connector",
        "config": {
          "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
          "database.hostname": "postgres",
          "database.port": "5432",
          "database.user": "postgres",
          "database.password": "Admin123",
          "database.dbname": "NFLScoreDatabase",
          "topic.prefix": "nflscoredatabase",
          "plugin.name": "pgoutput",
          "topic.creation.default.retention.ms": "-1",
          "topic.creation.default.partitions": "1",
          "topic.creation.default.replication.factor": "1"
        }
      }'