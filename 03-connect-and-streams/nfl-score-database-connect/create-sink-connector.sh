curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
        "name": "sink-connector",
        "config": {
          "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
          "connection.url": "jdbc:postgresql://postgres:5432/NFLScoreDatabase",
          "connection.username": "postgres",
          "connection.password": "Admin123",
          "primary.key.mode": "record_value",
          "primary.key.fields": "id",
          "schema.evolution": "basic",
          "topics": "nflscoredatabase.sink.game_result",
          "insert.mode": "upsert"
        }
      }'