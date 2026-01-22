package io.github.andygabler.nfl.result.consumer.jpa.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class GameResultEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;
    @Column(name = "home_team")
    private String homeTeam;
    @Column(name = "home_team_score")
    private Integer homeTeamScore;
    @Column(name = "visiting_team")
    private String visitingTeam;
    @Column(name = "visiting_team_score")
    private Integer visitingTeamScore;
}
