package io.github.andygabler.nflscorestreams.aggregation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

@Component
public class GameResultSumMapper implements ValueMapper<JsonNode, JsonNode> {
    @Override
    public JsonNode apply(JsonNode value) {
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode gameResult = mapper.createObjectNode();

        final long recordId = value.get("id").longValue();
        final long matchDate = value.get("match_date").longValue();
        final String homeTeamName = value.get("home_team").textValue();
        final String visitingTeamName = value.get("visiting_team").textValue();

        final HashSet<Long> scoreIds = new HashSet<>();
        final List<Score> scores = new ArrayList<>();
        value.get("scores").forEach(scoreNode -> {
            final long id = scoreNode.get("id").longValue();
            if (scoreIds.contains(id)) {
                return;
            }

            final Score score = new Score();
            score.snapType = scoreNode.get("snap_type").textValue();
            score.team = scoreNode.get("team").textValue();
            scoreIds.add(id);
            scores.add(score);
        });

        final int homeTeamScore = scores
            .stream()
            .mapToInt(score -> scoreToInt(score, homeTeamName))
            .sum();
        final int visitingTeamScore = scores
            .stream()
            .mapToInt(score -> scoreToInt(score, visitingTeamName))
            .sum();

        gameResult.put("id", recordId);
        gameResult.put("match_date", matchDate);
        gameResult.put("home_team", homeTeamName);
        gameResult.put("visiting_team", visitingTeamName);
        gameResult.put("home_team_score", homeTeamScore);
        gameResult.put("visiting_team_score", visitingTeamScore);

        return gameResult;
    }

    private static int scoreToInt(Score score, String teamToCheck) {
        if (!score.team.equalsIgnoreCase(teamToCheck)) {
            return 0;
        }
        System.out.println(score.snapType);

        return switch (score.snapType) {
            case "PASSING", "RUSHING" -> 6;
            case "EXTRA POINT KICK" -> 1;
            case "2-POINT CONVERSION" -> 2;
            case "FIELD GOAL" -> 3;
            default -> throw new IllegalArgumentException("Unknown score type " + score.snapType);
        };
    }

    private static class Score {
        String snapType;
        String team;
    }
}
