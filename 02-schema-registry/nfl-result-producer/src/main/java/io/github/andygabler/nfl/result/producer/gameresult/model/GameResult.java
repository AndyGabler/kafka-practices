package io.github.andygabler.nfl.result.producer.gameresult.model;

import lombok.Data;

import javax.validation.constraints.NotNull;
import java.util.Date;

@Data
public class GameResult {
    @NotNull
    private Team homeTeam;
    private int homeTeamScore;
    @NotNull
    private Team visitingTeam;
    private int visitingTeamScore;
    private Date datePlayed;
}
