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
    private StreamMaker gameAndScoreAggregatorStreamMaker;

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        scoreRekeyStreamMaker.buildPipeline(streamsBuilder);
        gameRekeyStreamMaker.buildPipeline(streamsBuilder);
        gameAndScoreAggregatorStreamMaker.buildPipeline(streamsBuilder);
    }
}
