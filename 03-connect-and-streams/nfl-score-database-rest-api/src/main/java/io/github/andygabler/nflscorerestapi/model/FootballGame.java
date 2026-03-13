package io.github.andygabler.nflscorerestapi.model;

import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDate;

@Data
@Entity
@Table(name = "football_game")
public class FootballGame {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "match_date")
    private LocalDate matchDate;

    @Column(name = "home_team")
    private String homeTeam;

    @Column(name = "visiting_team")
    private String visitingTeam;
}
