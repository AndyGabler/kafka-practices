package io.github.andygabler.nflscorestreams.rekey;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class GameRekeyMapperTest {
    private final GameRekeyMapper objectUnderTest = new GameRekeyMapper();

    @Test
    public void testApply_happyPath() throws IOException {
        final JsonNode inputNode = new ObjectMapper().readTree(
            GameRekeyMapperTest
                .class
                .getResource("/rekey/game-rekey/happy-path.json")
                .openStream()
        );
        final KeyValue<Long, JsonNode> result = objectUnderTest.apply("Struct{id=26}", inputNode);
        Assertions.assertEquals(26L, result.key);
        Assertions.assertNotNull(result.value);
        Assertions.assertEquals(26L, result.value.get("id").longValue());
        Assertions.assertEquals(20383, result.value.get("match_date").longValue());
        Assertions.assertEquals("Bills", result.value.get("home_team").textValue());
        Assertions.assertEquals("Falcons", result.value.get("visiting_team").textValue());
    }
}
