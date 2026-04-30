package io.github.andygabler.nflscorestreams.aggregation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class GameAndScoreAggregatorJoinerTest {
    private final GameAndScoreAggregatorJoiner objectUnderTest = new GameAndScoreAggregatorJoiner();

    @Test
    public void testApply_happyPath() throws URISyntaxException, IOException {
        final String scoreRekeyPayload = Files.readString(
            Paths.get(
                GameAndScoreAggregatorJoinerTest
                    .class
                    .getResource("/aggregation/score-rekey.json")
                    .toURI()
            )
        );

        final String gameRekeyPayload = Files.readString(
            Paths.get(
                GameAndScoreAggregatorJoinerTest
                    .class
                    .getResource("/aggregation/game-rekey.json")
                    .toURI()
            )
        );

        final JsonNode result = objectUnderTest.apply(gameRekeyPayload, scoreRekeyPayload);
        final JsonNode expectedResult = new ObjectMapper().readTree(
            GameAndScoreAggregatorJoinerTest
                .class
                .getResource("/aggregation/aggregation-result.json")
                .openStream()
        );

        Assertions.assertEquals(expectedResult, result);
    }
}
