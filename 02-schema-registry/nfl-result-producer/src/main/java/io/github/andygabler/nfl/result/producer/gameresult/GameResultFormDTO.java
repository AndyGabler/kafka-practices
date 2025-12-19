package io.github.andygabler.nfl.result.producer.gameresult;

import lombok.Data;

@Data
public class GameResultFormDTO {
    private String homeTeam;
    private int homeTeamScore;
    private String visitingTeam;
    private int visitingTeamScore;
}
