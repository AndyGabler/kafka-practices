package io.github.andygabler.nflscorestreams.aggregation;

import io.github.andygabler.nflscorestreams.StreamMaker;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GameAndScoreAggregatorStreamMaker implements StreamMaker {

    @Autowired
    private GameAndScoreAggregatorJoiner gameAndScoreAggregatorJoiner;

    @Autowired
    private GameResultSumMapper gameResultSumMapper;

    @Override
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        // TODO consider flushing state store more often. Maybe set to 0
        final KTable<Long, String> gameRekeyTable = streamsBuilder
            .table(
                "nflscoredatabase.public.football_game.rekey",
                    Materialized.with(Serdes.Long(), Serdes.String())
            );

        final KStream<Long, String> scoreRekeyStream = streamsBuilder
            .stream(
                "nflscoredatabase.public.game_score.rekey",
                    Consumed.with(Serdes.Long(), Serdes.String())
            );


        /*
         * We want a left join here. Stream will throw exception that will be retried if KTable value is late.
         */
        scoreRekeyStream
            .leftJoin(gameRekeyTable, gameAndScoreAggregatorJoiner)
            .mapValues(gameResultSumMapper)
            .to("nflscoredatabase.sink.game_result");
    }
}
