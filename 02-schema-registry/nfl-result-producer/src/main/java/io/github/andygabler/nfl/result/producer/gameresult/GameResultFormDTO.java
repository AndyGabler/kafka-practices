package io.github.andygabler.nfl.result.producer.gameresult;

import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

import java.util.Date;

@Data
public class GameResultFormDTO {
    private String homeTeam;
    private int homeTeamScore;
    private String visitingTeam;
    private int visitingTeamScore;
    @DateTimeFormat(pattern = "yyyy-MM-dd")
    private Date datePlayed;
}
