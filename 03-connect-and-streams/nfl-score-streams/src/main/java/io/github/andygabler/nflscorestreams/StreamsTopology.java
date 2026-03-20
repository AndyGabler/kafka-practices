package io.github.andygabler.nflscorestreams;

import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StreamsTopology {

    @Autowired
    private StreamMaker scoreRekeyStreamMaker;

    @Autowired
    private StreamMaker gameRekeyStreamMaker;

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        scoreRekeyStreamMaker.buildPipeline(streamsBuilder);
        gameRekeyStreamMaker.buildPipeline(streamsBuilder);

        /*
         * Re-Key games. Simply parse out the key from Struct{id=2} to integer 2.
         */
        /*final KStream<String, String> gameRekeyStream = streamsBuilder
            .stream(
            "nflscoredatabase.public.football_game",
                Consumed.with(Serdes.String(), Serdes.String())
            );
        gameRekeyStream
            .mapValues((key, value) ->
                KeyValue.pair(RekeyUtil.parseDebeziumKey(key), value)
            ).to("streams.football_game.rekey");*/

    }
}
