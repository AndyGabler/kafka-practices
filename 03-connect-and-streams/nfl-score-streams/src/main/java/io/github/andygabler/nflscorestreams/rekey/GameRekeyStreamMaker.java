package io.github.andygabler.nflscorestreams.rekey;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.andygabler.nflscorestreams.StreamMaker;
import io.github.andygabler.nflscorestreams.util.DebeziumCreateOnlyFilter;
import io.github.andygabler.nflscorestreams.util.JsonNodeFlatMapper;
import io.github.andygabler.nflscorestreams.util.RekeyUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GameRekeyStreamMaker implements StreamMaker {

    @Autowired
    private DebeziumCreateOnlyFilter debeziumCreateOnlyFilter;

    @Autowired
    private GameRekeyMapper gameRekeyMapper;

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
            .to("nflscoredatabase.public.football_game.rekey");
    }
}
