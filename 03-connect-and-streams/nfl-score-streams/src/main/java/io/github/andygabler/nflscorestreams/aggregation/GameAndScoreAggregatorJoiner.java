package io.github.andygabler.nflscorestreams.aggregation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.SneakyThrows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class GameAndScoreAggregatorJoiner implements ValueJoiner<String, String, JsonNode> {
    @SneakyThrows
    @Override
    public JsonNode apply(String gameRekeyRecord, String scoreAggregateRecord) {
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode gameRekeyJson = objectMapper.readTree(gameRekeyRecord);

        if (scoreAggregateRecord == null) {
            return gameRekeyJson;
        }

        final ArrayNode scoreArray = new ObjectMapper().readValue(scoreAggregateRecord, ArrayNode.class);
        scoreArray.forEach(scoreRekeyJson -> {
            ((ArrayNode) gameRekeyJson.get("scores")).add(scoreRekeyJson);
        });
        return gameRekeyJson;
    }
}
