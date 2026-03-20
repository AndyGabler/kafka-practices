package io.github.andygabler.nflscorestreams.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Collections;
import java.util.List;

public class JsonNodeFlatMapper<KEY_TYPE> implements KeyValueMapper<KEY_TYPE, String, List<KeyValue<KEY_TYPE, JsonNode>>> {
    @Override
    public List<KeyValue<KEY_TYPE, JsonNode>> apply(KEY_TYPE key, String value) {
        if (value == null) {
            return Collections.emptyList();
        }

        try {
            return Collections.singletonList(new KeyValue<>(key, new ObjectMapper().readTree(value)));
        } catch (JsonProcessingException exception) {
            return Collections.emptyList();
        }
    }
}
