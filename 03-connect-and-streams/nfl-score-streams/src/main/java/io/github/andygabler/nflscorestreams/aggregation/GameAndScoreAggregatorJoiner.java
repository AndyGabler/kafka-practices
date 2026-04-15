package io.github.andygabler.nflscorestreams.aggregation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.SneakyThrows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.stereotype.Component;

@Component
public class GameAndScoreAggregatorJoiner implements ValueJoiner<String, String, JsonNode> {
    @SneakyThrows
    @Override
    public JsonNode apply(String scoreRekeyRecord, String gameRekeyRecord) {
        if (gameRekeyRecord == null) {
            throw new AggregationException("gameRekeyRecord was null. Indicating a late arriving value.");
        }

        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode scoreRekeyJson = objectMapper.readTree(scoreRekeyRecord);
        final JsonNode gameRekeyJson = objectMapper.readTree(gameRekeyRecord);

        ((ArrayNode) gameRekeyJson.get("scores")).add(scoreRekeyJson);
        return gameRekeyJson;
    }
}
