package io.github.andygabler.nflscorestreams.rekey;

import io.github.andygabler.nflscorestreams.StreamMaker;
import io.github.andygabler.nflscorestreams.util.DebeziumCreateOnlyFilter;
import io.github.andygabler.nflscorestreams.util.JsonNodeFlatMapper;
import io.github.andygabler.nflscorestreams.util.JsonNodeToStringMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Makes a stream that takes football game topic messages and does the following:
 *  - Transforms the key to a long
 *  - Extracts the Debezium payload to just be the table content
 *  - Adds a struct to the game JSON to include its scores
 */
@Component
public class GameRekeyStreamMaker implements StreamMaker {

    @Autowired
    private DebeziumCreateOnlyFilter debeziumCreateOnlyFilter;

    @Autowired
    private GameRekeyMapper gameRekeyMapper;

    @Autowired
    private GameScoreJsonArrayCreatorMapper gameScoreJsonArrayCreatorMapper;

    @Autowired
    private JsonNodeToStringMapper jsonNodeToStringMapper;

    @Override
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        final KStream<String, String> gameRekeyStream = streamsBuilder
            .stream(
                "nflscoredatabase.public.football_game",
                Consumed.with(Serdes.String(), Serdes.String())
            );
        gameRekeyStream
            .flatMap(new JsonNodeFlatMapper<>())
            .filter(debeziumCreateOnlyFilter)
            .map(gameRekeyMapper)
            .mapValues(gameScoreJsonArrayCreatorMapper)
            .mapValues(jsonNodeToStringMapper)
            .to("nflscoredatabase.public.football_game.rekey", Produced.with(Serdes.Long(), Serdes.String()));
    }
}
