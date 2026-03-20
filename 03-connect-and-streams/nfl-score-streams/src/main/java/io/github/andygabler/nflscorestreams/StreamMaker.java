package io.github.andygabler.nflscorestreams;

import org.apache.kafka.streams.StreamsBuilder;

public interface StreamMaker {
    // TODO enable switch?
    void buildPipeline(StreamsBuilder streamsBuilder);
}
