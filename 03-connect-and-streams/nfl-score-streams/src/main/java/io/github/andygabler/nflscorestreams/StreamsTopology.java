package io.github.andygabler.nflscorestreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StreamsTopology {

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        final KStream<String, String> databaseChangelogStream = streamsBuilder
            .stream(
                "nflscoredatabase.public.football_game",
                    Consumed.with(Serdes.String(), Serdes.String())
            );
        databaseChangelogStream.foreach((key, value) -> {
            System.out.println("Key: " + key);
            System.out.println("Value: " + value);
        });
    }
}
