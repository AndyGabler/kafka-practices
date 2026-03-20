package io.github.andygabler.nflscorestreams.rekey;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.stereotype.Component;

@Component
public class ScoreRekeyMapper implements KeyValueMapper<String, JsonNode, KeyValue<Long, JsonNode>> {
    @Override
    public KeyValue<Long, JsonNode> apply(String key, JsonNode value) {
        final JsonNode payload = value.get("payload").get("after");
        final long gameId = payload.get("football_game_id").longValue();
        return KeyValue.pair(gameId, payload);
    }
}
