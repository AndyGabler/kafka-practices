package io.github.andygabler.nfl.result.producer.gameresult.model;

import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
public class GameResult {
    @NotNull
    private Team homeTeam;
    private int homeTeamScore;
    @NotNull
    private Team visitingTeam;
    private int visitingTeamScore;
}
