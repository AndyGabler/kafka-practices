package io.github.andygabler.nflscorestreams.model;

import lombok.Data;

@Data
public class GameScore {
    private Long id;
    private Long footballGameId;
    private String quarterback;
    private String ballCarrier;
    private String snapType;
    private String team;

    @Override
    public String toString() {
        return "[" +
                "\n\tid=" +
                id +
                "\n\tfootballGameId=" +
                footballGameId +
                "\n\tquarterback=" +
                quarterback +
                "\n\tballCarrier=" +
                ballCarrier +
                "\n\tsnapType=" +
                snapType +
                "\n\tteam=" +
                team +
                "\n]";
    }
}
