package io.github.andygabler.nfl.result.producer.gameresult;

import lombok.Data;

@Data
public class GameResult {
    private String homeTeam;
    private int homeTeamScore;
    private String visitingTeam;
    private int visitingTeamScore;
}
