package io.github.andygabler.nflscorestreams.rekey;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.andygabler.nflscorestreams.util.RekeyUtil;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.stereotype.Component;

@Component
public class GameRekeyMapper implements KeyValueMapper<String, JsonNode, KeyValue<Long, JsonNode>> {
    @Override
    public KeyValue<Long, JsonNode> apply(String key, JsonNode value) {
        final JsonNode payload = value.get("payload").get("after");
        final long gameId = RekeyUtil.parseDebeziumKey(key);
        return KeyValue.pair(gameId, payload);
    }
}
