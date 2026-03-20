package io.github.andygabler.nflscorestreams.rekey;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ScoreRekeyMapperTest {
    private final ScoreRekeyMapper objectUnderTest = new ScoreRekeyMapper();

    @Test
    public void testApply_happyPath() throws IOException {
        final JsonNode inputNode = new ObjectMapper().readTree(
            ScoreRekeyMapper
                .class
                .getResource("/rekey/score-rekey/happy-path.json")
                .openStream()
        );

        final KeyValue<Long, JsonNode> result = objectUnderTest.apply("Struct{id=363}", inputNode);
        Assertions.assertEquals(14L, result.key);
        Assertions.assertEquals(363L, result.value.get("id").longValue());
        Assertions.assertEquals(14L, result.value.get("football_game_id").longValue());
        Assertions.assertEquals("Joe Flacco", result.value.get("quarterback").textValue());
        Assertions.assertEquals("Evan McPherson", result.value.get("ball_carrier").textValue());
        Assertions.assertEquals("EXTRA POINT KICK", result.value.get("snap_type").textValue());
        Assertions.assertEquals("Bengals", result.value.get("team").textValue());
    }
}
