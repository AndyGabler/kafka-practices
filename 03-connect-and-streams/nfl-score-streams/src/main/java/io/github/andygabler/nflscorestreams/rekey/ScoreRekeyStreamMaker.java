package io.github.andygabler.nflscorestreams.rekey;

import io.github.andygabler.nflscorestreams.StreamMaker;
import io.github.andygabler.nflscorestreams.util.DebeziumCreateOnlyFilter;
import io.github.andygabler.nflscorestreams.util.JsonNodeFlatMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Makes a stream that rekeys items on the game_score topic. Makes the AFTER payload the new message and
 * makes the ID of the football game the new key.
 *
 * Makes a stream that takes football game topic messages and does the following:
 *  - Changes the keys of items on the game score topic to be the key of the football game
 *  - Extracts the Debezium payload to just be the table content
 */
@Component
public class ScoreRekeyStreamMaker implements StreamMaker {

    @Autowired
    private DebeziumCreateOnlyFilter debeziumCreateOnlyFilter;

    @Autowired
    private ScoreRekeyMapper scoreRekeyMapper;

    @Autowired
    private DebeziumPayloadExtractionMapper debeziumPayloadExtractionMapper;

    @Override
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        final KStream<String, String> scoreRekeyStream = streamsBuilder
            .stream(
                "nflscoredatabase.public.game_score",
                Consumed.with(Serdes.String(), Serdes.String())
            );
        scoreRekeyStream
            .flatMap(new JsonNodeFlatMapper<>())
            .filter(debeziumCreateOnlyFilter)
            .map(scoreRekeyMapper)
            .mapValues(debeziumPayloadExtractionMapper)
            .to("nflscoredatabase.public.game_score.rekey");
    }
}
