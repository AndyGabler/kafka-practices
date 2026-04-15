package io.github.andygabler.nflscorestreams.aggregation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class GameResultSumMapperTest {
    private final GameResultSumMapper objectUnderTest = new GameResultSumMapper();

    @Test
    public void testApply_badSnapType() throws IOException {
        final JsonNode input = new ObjectMapper().readTree(
            GameResultSumMapperTest
                .class
                .getResource("/aggregation/result-sum/bad-snap-type.json")
                .openStream()
        );

        Assertions.assertThrows(IllegalArgumentException.class, () -> objectUnderTest.apply(input));
    }

    @Test
    public void testApply_noScores() throws IOException {
        final JsonNode input = new ObjectMapper().readTree(
            GameResultSumMapperTest
                .class
                .getResource("/aggregation/result-sum/no-scores.json")
                .openStream()
        );

        final JsonNode result = objectUnderTest.apply(input);
        Assertions.assertEquals(1L, result.get("id").longValue());
        Assertions.assertEquals("Titans", result.get("home_team").textValue());
        Assertions.assertEquals("Browns", result.get("visiting_team").textValue());
        Assertions.assertEquals(0, result.get("home_team_score").intValue());
        Assertions.assertEquals(0, result.get("visiting_team_score").intValue());
    }

    @Test
    public void testApply_happyPath() throws IOException {
        final JsonNode input = new ObjectMapper().readTree(
            GameResultSumMapperTest
                .class
                .getResource("/aggregation/result-sum/happy-path.json")
                .openStream()
        );

        final JsonNode result = objectUnderTest.apply(input);
        Assertions.assertEquals(1L, result.get("id").longValue());
        Assertions.assertEquals("Packers", result.get("home_team").textValue());
        Assertions.assertEquals("Lions", result.get("visiting_team").textValue());
        Assertions.assertEquals(3, result.get("home_team_score").intValue());
        Assertions.assertEquals(15, result.get("visiting_team_score").intValue());
    }
}
