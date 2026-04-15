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
         * You will want a Left Join stream.
         * Emits an output for each record in the left or primary input source.
         * If the other source does not have a value for a given key, it is set to null.
         *
         * We know a football game will exist first so never can be null if it is on the right. The left source will
         * be the game score
         */
    }
}
