package io.github.andygabler.nfl.result.consumer.kafka.model;

import lombok.Data;

@Data
public class ConsumedGameResult {
    private String homeTeam;
    private Integer homeTeamScore;
    private String visitingTeam;
    private Integer visitingTeamScore;
    private String datePlayed;
}
