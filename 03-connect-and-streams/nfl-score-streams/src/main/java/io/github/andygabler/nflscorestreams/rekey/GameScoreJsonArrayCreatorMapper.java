package io.github.andygabler.nflscorestreams.rekey;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.stereotype.Component;

@Component
public class GameScoreJsonArrayCreatorMapper implements ValueMapper<JsonNode, JsonNode> {
    @Override
    public JsonNode apply(JsonNode value) {
        value.withArrayProperty("scores");
        return value;
    }
}
