package io.github.andygabler.nflscorerestapi.model;

import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
@Table(name = "game_score")
public class GameScore {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "football_game_id")
    private Long footballGameId;

    @Column(name = "quarterback")
    private String quarterback;

    @Column(name = "ball_carrier")
    private String ballCarrier;

    @Column(name = "snap_type")
    private String snapType;

    @Column(name = "team")
    private String team;
}
