package io.github.andygabler.nflscorestreams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
        databaseChangelogStream
            // Filter tombstone records. We are unfortunately not interested.
            .filter((key, value) -> value != null)
            .mapValues((key, value) -> {
                try {
                    return new ObjectMapper().readTree(value);
                } catch (JsonProcessingException exception) {
                    return null;
                }
            })
            // These are straight from Debezium so they should parse, all the same, filter it out
            .filter((key, value) -> value != null)
            .foreach((key, value) -> {
                System.out.println("Key2: " + key);
                System.out.println("Value2: " + value);
            });
    }
}
