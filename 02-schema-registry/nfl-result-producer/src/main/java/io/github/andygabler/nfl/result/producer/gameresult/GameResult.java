package io.github.andygabler.nfl.result.producer.gameresult;

import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
public class GameResult {
    @NotNull
    private String homeTeam;
    private int homeTeamScore;
    @NotNull
    private String visitingTeam;
    private int visitingTeamScore;
}
