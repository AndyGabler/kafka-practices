package io.github.andygabler.nflscorestreams.aggregation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;

public class GameResultToDebeziumSinkMapperTest {
    private final GameResultToDebeziumSinkMapper objectUnderTest = new GameResultToDebeziumSinkMapper();

    @Test
    public void testApply_happyPath() throws IOException, JSONException {
        final JsonNode input = new ObjectMapper().readTree(
            GameResultToDebeziumSinkMapperTest
                .class
                .getResource("/aggregation/debezium-wrap/game-result-input.json")
                .openStream()
        );

        final JsonNode result = objectUnderTest.apply(input);
        JSONAssert.assertEquals(
            new ObjectMapper().readTree(
                GameResultToDebeziumSinkMapperTest
                    .class
                    .getResource("/aggregation/debezium-wrap/debezium-sink-message.json")
                    .openStream()
            ).toString(),
            result.toString(),
            false
        );
    }
}
