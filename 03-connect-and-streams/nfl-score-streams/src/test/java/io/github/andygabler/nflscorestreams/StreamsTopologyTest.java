package io.github.andygabler.nflscorestreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.json.JSONException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@SpringBootTest(
    "spring.kafka.streams.auto-startup=false"
)
public class StreamsTopologyTest {

    private static final String GAME_TOPIC = "nflscoredatabase.public.football_game";
    private static final String SCORE_TOPIC = "nflscoredatabase.public.game_score";
    private static final String GAME_REKEY_TOPIC = "nflscoredatabase.public.football_game.rekey";
    private static final String SCORE_REKEY_TOPIC = "nflscoredatabase.public.game_score.rekey";
    private static final String RESULT_TOPIC = "nflscoredatabase.sink.game_result";
    private static final String GAME_SCORE_JOIN_TOPIC = "nflscoredatabase.public.score_and_game_join";

    @Autowired
    private StreamsTopology streamsTopology;

    private TopologyTestDriver topologyDriver;

    private TestInputTopic<String, String> gameTopic;
    private TestInputTopic<String, String> scoreTopic;
    private TestOutputTopic<Long, String> gameRekeyTopic;
    private TestOutputTopic<Long, String> scoreRekeyTopic;
    private TestOutputTopic<Long, String> gameAndScoreJoin;
    private TestOutputTopic<Long, String> gameResultTopic;

    @BeforeEach
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        streamsTopology.buildPipeline(builder);
        final Topology topology = builder.build();
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        topologyDriver = new TopologyTestDriver(topology, properties);

        gameTopic = topologyDriver.createInputTopic(GAME_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        scoreTopic = topologyDriver.createInputTopic(SCORE_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        gameRekeyTopic = topologyDriver.createOutputTopic(GAME_REKEY_TOPIC, Serdes.Long().deserializer(), Serdes.String().deserializer());
        scoreRekeyTopic = topologyDriver.createOutputTopic(SCORE_REKEY_TOPIC, Serdes.Long().deserializer(), Serdes.String().deserializer());
        gameAndScoreJoin = topologyDriver.createOutputTopic(GAME_SCORE_JOIN_TOPIC, Serdes.Long().deserializer(), Serdes.String().deserializer());
        gameResultTopic = topologyDriver.createOutputTopic(RESULT_TOPIC, Serdes.Long().deserializer(), Serdes.String().deserializer());

    }

    @AfterEach
    public void cleanup() {
        topologyDriver.close();
    }

    @Test
    public void testScoreAggregationPipeline() throws IOException, URISyntaxException, InterruptedException, JSONException {
        gameTopic.pipeInput("Struct{id=1}", getPayload("/topology-test/debezium-game-messages/game1.json"));
        gameTopic.pipeInput("Struct{id=2}", getPayload("/topology-test/debezium-game-messages/game2.json"));

        scoreTopic.pipeInput("Struct{id=1}", getPayload("/topology-test/debezium-score-messages/score1.json"));
        scoreTopic.pipeInput("Struct{id=2}", getPayload("/topology-test/debezium-score-messages/score2.json"));
        scoreTopic.pipeInput("Struct{id=3}", getPayload("/topology-test/debezium-score-messages/score3.json"));
        scoreTopic.pipeInput("Struct{id=4}", getPayload("/topology-test/debezium-score-messages/score4.json"));
        scoreTopic.pipeInput("Struct{id=5}", getPayload("/topology-test/debezium-score-messages/score5.json"));
        scoreTopic.pipeInput("Struct{id=6}", getPayload("/topology-test/debezium-score-messages/score6.json"));
        scoreTopic.pipeInput("Struct{id=7}", getPayload("/topology-test/debezium-score-messages/score7.json"));

        scoreTopic.pipeInput("Struct{id=8}", getPayload("/topology-test/debezium-score-messages/score8.json"));
        scoreTopic.pipeInput("Struct{id=9}", getPayload("/topology-test/debezium-score-messages/score9.json"));
        scoreTopic.pipeInput("Struct{id=10}", getPayload("/topology-test/debezium-score-messages/score10.json"));
        scoreTopic.pipeInput("Struct{id=11}", getPayload("/topology-test/debezium-score-messages/score11.json"));

        // Verify Game Rekeys
        final List<TestRecord<Long, String>> gameRekeyRecords = gameRekeyTopic.readRecordsToList();
        Assertions.assertEquals(2, gameRekeyRecords.size());
        
        final TestRecord<Long, String> gameRekeyRecord0 = gameRekeyRecords.get(0);
        Assertions.assertEquals(1L, gameRekeyRecord0.key());
        JSONAssert.assertEquals(getPayload("/topology-test/game-rekeys/message1.json"), gameRekeyRecord0.value(), false);

        final TestRecord<Long, String> gameRekeyRecord1 = gameRekeyRecords.get(1);
        Assertions.assertEquals(2L, gameRekeyRecord1.key());
        JSONAssert.assertEquals(getPayload("/topology-test/game-rekeys/message2.json"), gameRekeyRecord1.value(), false);

        // Verify Score Rekeys
        final List<TestRecord<Long, String>> scoreRekeyRecords = scoreRekeyTopic.readRecordsToList();
        Assertions.assertEquals(11, scoreRekeyRecords.size());
        
        final TestRecord<Long, String> scoreRekeyRecord0 = scoreRekeyRecords.get(0);
        Assertions.assertEquals(1L, scoreRekeyRecord0.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message1.json"), scoreRekeyRecord0.value(), false);

        final TestRecord<Long, String> scoreRekeyRecord1 = scoreRekeyRecords.get(1);
        Assertions.assertEquals(1L, scoreRekeyRecord1.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message2.json"), scoreRekeyRecord1.value(), false);

        final TestRecord<Long, String> scoreRekeyRecord2 = scoreRekeyRecords.get(2);
        Assertions.assertEquals(1L, scoreRekeyRecord2.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message3.json"), scoreRekeyRecord2.value(), false);

        final TestRecord<Long, String> scoreRekeyRecord3 = scoreRekeyRecords.get(3);
        Assertions.assertEquals(1L, scoreRekeyRecord3.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message4.json"), scoreRekeyRecord3.value(), false);

        final TestRecord<Long, String> scoreRekeyRecord4 = scoreRekeyRecords.get(4);
        Assertions.assertEquals(1L, scoreRekeyRecord4.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message5.json"), scoreRekeyRecord4.value(), false);

        final TestRecord<Long, String> scoreRekeyRecord5 = scoreRekeyRecords.get(5);
        Assertions.assertEquals(1L, scoreRekeyRecord5.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message6.json"), scoreRekeyRecord5.value(), false);

        final TestRecord<Long, String> scoreRekeyRecord6 = scoreRekeyRecords.get(6);
        Assertions.assertEquals(1L, scoreRekeyRecord6.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message7.json"), scoreRekeyRecord6.value(), false);

        final TestRecord<Long, String> scoreRekeyRecord7 = scoreRekeyRecords.get(7);
        Assertions.assertEquals(2L, scoreRekeyRecord7.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message8.json"), scoreRekeyRecord7.value(), false);

        final TestRecord<Long, String> scoreRekeyRecord8 = scoreRekeyRecords.get(8);
        Assertions.assertEquals(2L, scoreRekeyRecord8.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message9.json"), scoreRekeyRecord8.value(), false);

        final TestRecord<Long, String> scoreRekeyRecord9 = scoreRekeyRecords.get(9);
        Assertions.assertEquals(2L, scoreRekeyRecord9.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message10.json"), scoreRekeyRecord9.value(), false);

        final TestRecord<Long, String> scoreRekeyRecord10 = scoreRekeyRecords.get(10);
        Assertions.assertEquals(2L, scoreRekeyRecord10.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message11.json"), scoreRekeyRecord10.value(), false);

        // Verify joins
        final List<TestRecord<Long, String>> joinRecords = gameAndScoreJoin.readRecordsToList();

        final Optional<TestRecord<Long, String>> game1Aggregate = joinRecords
            .stream()
            .filter(record -> record.key() == 1L)
            .reduce((first, second) -> second);

        Assertions.assertTrue(game1Aggregate.isPresent());
        JSONAssert.assertEquals(getPayload("/topology-test/joins/game1aggregate.json"), game1Aggregate.get().value(), false);

        final Optional<TestRecord<Long, String>> game2Aggregate = joinRecords
                .stream()
                .filter(record -> record.key() == 2L)
                .reduce((first, second) -> second);

        Assertions.assertTrue(game2Aggregate.isPresent());
        JSONAssert.assertEquals(getPayload("/topology-test/joins/game2Aggregate.json"), game2Aggregate.get().value(), false);

        // Verify results
        final List<TestRecord<Long, String>> resultRecords = gameResultTopic.readRecordsToList();

        final Optional<TestRecord<Long, String>> game1Result = resultRecords
            .stream()
            .filter(record -> record.key() == 1L)
            .reduce((first, second) -> second);
        Assertions.assertTrue(game1Result.isPresent());
        JSONAssert.assertEquals(getPayload("/topology-test/results/game1result.json"), game1Result.get().value(), false);

        final Optional<TestRecord<Long, String>> game2Result = resultRecords
                .stream()
                .filter(record -> record.key() == 2L)
                .reduce((first, second) -> second);
        Assertions.assertTrue(game2Result.isPresent());
        JSONAssert.assertEquals(getPayload("/topology-test/results/game2result.json"), game2Result.get().value(), false);
    }

    private String getPayload(String resource) throws IOException, URISyntaxException {
        return Files.readString(
            Paths.get(
                Objects.requireNonNull(
                    StreamsTopologyTest.class.getResource(resource)
                ).toURI()
            )
        );
    }
}
