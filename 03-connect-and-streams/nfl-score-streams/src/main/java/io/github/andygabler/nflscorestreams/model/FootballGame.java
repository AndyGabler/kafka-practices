package io.github.andygabler.nflscorestreams.model;

import lombok.Data;

import java.time.LocalDate;

@Data
public class FootballGame {
    private Long id;
    private LocalDate matchDate;
    private String homeTeam;
    private String visitingTeam;

    @Override
    public String toString() {
        return "[" +
                "\n\tid=" +
                id +
                "\n\tmatchDate=" +
                matchDate +
                "\n\thomeTeam=" +
                homeTeam +
                "\n\tvisitingTeam=" +
                visitingTeam +
                "\n]";
    }
}
