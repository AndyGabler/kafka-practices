package io.github.andygabler.nflscorestreams.rekey;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class GameScoreJsonArrayCreatorMapperTest {
    private final GameScoreJsonArrayCreatorMapper objectUnderTest = new GameScoreJsonArrayCreatorMapper();

    @Test
    public void testApply() throws IOException {
        final JsonNode inputNode = new ObjectMapper().readTree(
            GameScoreJsonArrayCreatorMapper
                .class
                .getResource("/rekey/array-creator/happy-path.json")
                .openStream()
        );

        final JsonNode result = objectUnderTest.apply(inputNode);
        final JsonNode expectedResult = new ObjectMapper().readTree(
            GameScoreJsonArrayCreatorMapper
                .class
                .getResource("/rekey/array-creator/happy-path-expected-result.json")
                .openStream()
        );
        Assertions.assertEquals(expectedResult, result);
    }
}
