package io.github.andygabler.nflscorestreams.aggregation;

import io.github.andygabler.nflscorestreams.StreamMaker;
import io.github.andygabler.nflscorestreams.util.JsonNodeFlatMapper;
import io.github.andygabler.nflscorestreams.util.JsonNodeToStringMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GameAndScoreAggregatorStreamMaker implements StreamMaker {

    @Autowired
    private GameAndScoreAggregatorJoiner gameAndScoreAggregatorJoiner;

    @Autowired
    private GameResultSumMapper gameResultSumMapper;

    @Autowired
    private JsonNodeToStringMapper jsonNodeToStringMapper;

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
            .mapValues(jsonNodeToStringMapper)
            .to("nflscoredatabase.public.score_and_game_join", Produced.with(Serdes.Long(), Serdes.String()));

        // TODO could be ktable
        final KStream<Long, String> joinStream = streamsBuilder
            .stream(
                "nflscoredatabase.public.score_and_game_join",
                Consumed.with(Serdes.Long(), Serdes.String())
            );

        joinStream
            .flatMap(new JsonNodeFlatMapper<>())
            .mapValues(gameResultSumMapper)
            .mapValues(jsonNodeToStringMapper)
            .to("nflscoredatabase.sink.game_result", Produced.with(Serdes.Long(), Serdes.String()));
    }
}
