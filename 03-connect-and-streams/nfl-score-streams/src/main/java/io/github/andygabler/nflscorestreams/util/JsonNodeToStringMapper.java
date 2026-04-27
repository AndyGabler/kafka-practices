package io.github.andygabler.nflscorestreams.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.stereotype.Component;

@Component
public class JsonNodeToStringMapper implements ValueMapper<JsonNode, String> {
    @Override
    public String apply(JsonNode value) {
        return value.toString();
    }
}
