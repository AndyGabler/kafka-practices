package io.github.andygabler.nflscorestreams.aggregation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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

        final KTable<Long, String> scoreAggregateTable = scoreRekeyStream
            .groupByKey(Grouped.with(Serdes.Long(), Serdes.String()))
            .aggregate(
                // todo class with unit test
                () -> "[]",
                (key, value, aggregation) -> {
                    try {
                        final ObjectMapper objectReader = new ObjectMapper();
                        final ArrayNode scoreArray = objectReader.readValue(aggregation, ArrayNode.class);
                        scoreArray.add(objectReader.readValue(value, JsonNode.class));
                        return scoreArray.toString();
                    } catch (Exception exception) {
                        exception.printStackTrace();
                        return aggregation.toString();
                    }
                }, Materialized.with(Serdes.Long(), Serdes.String()));

        gameRekeyTable
            .leftJoin(scoreAggregateTable, gameAndScoreAggregatorJoiner)
            .mapValues(jsonNodeToStringMapper)
            .toStream()
            .to("nflscoredatabase.public.score_and_game_join", Produced.with(Serdes.Long(), Serdes.String()));

        final KStream<Long, String> scoreAndGameJoinStream =
            streamsBuilder
                .stream(
                    "nflscoredatabase.public.score_and_game_join",
                    Consumed.with(Serdes.Long(), Serdes.String())
                );

        scoreAndGameJoinStream
            .flatMap(new JsonNodeFlatMapper<>())
            .mapValues(gameResultSumMapper)
            .mapValues(jsonNodeToStringMapper)
            .to("nflscoredatabase.sink.game_result", Produced.with(Serdes.Long(), Serdes.String()));
    }
}
