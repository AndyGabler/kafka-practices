package io.github.andygabler.nflscorestreams.rekey;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class DebeziumPayloadExtractionMapperTest {
    private final DebeziumPayloadExtractionMapper objectUnderTest = new DebeziumPayloadExtractionMapper();


    @Test
    public void testApply() throws IOException {
        final JsonNode inputNode = new ObjectMapper().readTree(
            DebeziumPayloadExtractionMapperTest
                .class
                .getResource("/rekey/game-rekey/happy-path.json")
                .openStream()
        );
        final JsonNode result = objectUnderTest.apply(inputNode);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(26L, result.get("id").longValue());
        Assertions.assertEquals(20383, result.get("match_date").longValue());
        Assertions.assertEquals("Bills", result.get("home_team").textValue());
        Assertions.assertEquals("Falcons", result.get("visiting_team").textValue());
    }
}
