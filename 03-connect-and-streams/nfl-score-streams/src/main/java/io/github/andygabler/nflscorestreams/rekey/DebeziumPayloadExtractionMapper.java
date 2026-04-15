package io.github.andygabler.nflscorestreams.rekey;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.stereotype.Component;

@Component
public class DebeziumPayloadExtractionMapper implements ValueMapper<JsonNode, JsonNode> {
    @Override
    public JsonNode apply(JsonNode value) {
        return value.get("payload").get("after");
    }
}
