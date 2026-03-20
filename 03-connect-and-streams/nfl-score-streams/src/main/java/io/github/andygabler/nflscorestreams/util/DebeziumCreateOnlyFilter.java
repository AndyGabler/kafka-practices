package io.github.andygabler.nflscorestreams.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.stereotype.Component;

@Component
public class DebeziumCreateOnlyFilter implements Predicate<String, JsonNode> {
    @Override
    public boolean test(String key, JsonNode value) {
        final String operation = value.get("payload").get("op").textValue();
        return operation != null && operation.equalsIgnoreCase("c");
    }
}
