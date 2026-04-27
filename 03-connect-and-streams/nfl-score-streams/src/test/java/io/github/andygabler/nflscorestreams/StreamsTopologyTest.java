package io.github.andygabler.nflscorestreams;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

@SpringBootTest
@DirtiesContext // todo don't think this is needed
@EmbeddedKafka(
    partitions = 1,
    topics = {
        "nflscoredatabase.public.football_game",
        "nflscoredatabase.public.football_game.rekey",
        "nflscoredatabase.public.game_score",
        "nflscoredatabase.public.game_score.rekey",
        "nflscoredatabase.public.score_and_game_join",
        "nflscoredatabase.sink.game_result"
    }
)
public class StreamsTopologyTest {

    private static final String GAME_TOPIC = "nflscoredatabase.public.football_game";
    private static final String SCORE_TOPIC = "nflscoredatabase.public.game_score";
    private static final String GAME_REKEY_TOPIC = "nflscoredatabase.public.football_game.rekey";
    private static final String SCORE_REKEY_TOPIC = "nflscoredatabase.public.game_score.rekey";
    private static final String RESULT_TOPIC = "nflscoredatabase.sink.game_result";
    private static final String GAME_SCORE_JOIN_TOPIC = "nflscoredatabase.public.score_and_game_join";

    @Autowired
    private EmbeddedKafkaBroker broker;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    private Consumer<Long, String> gameRekeyTopicConsumer;
    private Consumer<Long, String> scoreRekeyTopicConsumer;
    private Consumer<Long, String> gameAndScoreJoinConsumer;
    private Consumer<Long, String> gameResultTopicConsumer;

    @BeforeEach
    public void setup() {
        gameRekeyTopicConsumer = createConsumer();
        scoreRekeyTopicConsumer = createConsumer();
        gameAndScoreJoinConsumer = createConsumer();
        gameResultTopicConsumer = createConsumer();

        broker.consumeFromAnEmbeddedTopic(
            gameRekeyTopicConsumer, GAME_REKEY_TOPIC
        );
        broker.consumeFromAnEmbeddedTopic(
            scoreRekeyTopicConsumer, SCORE_REKEY_TOPIC
        );
        broker.consumeFromAnEmbeddedTopic(
            gameAndScoreJoinConsumer, GAME_SCORE_JOIN_TOPIC
        );
        broker.consumeFromAnEmbeddedTopic(
            gameResultTopicConsumer, RESULT_TOPIC
        );
    }

    private Consumer<Long, String> createConsumer() {
        final Map<String, Object> consumerProperties = KafkaTestUtils
                .consumerProps(broker, UUID.randomUUID().toString(), true);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<Long, String>(consumerProperties).createConsumer();
    }

    @Test
    public void testScoreAggregationPipeline() throws IOException, URISyntaxException, InterruptedException, JSONException {
        kafkaTemplate.send(GAME_TOPIC, "Struct{id=1}", getPayload("/topology-test/debezium-game-messages/game1.json"));
        // TODO throw this one on the end for funsies
        kafkaTemplate.send(GAME_TOPIC, "Struct{id=2}", getPayload("/topology-test/debezium-game-messages/game2.json"));

        kafkaTemplate.send(SCORE_TOPIC, "Struct{id=1}", getPayload("/topology-test/debezium-score-messages/score1.json"));
        kafkaTemplate.send(SCORE_TOPIC, "Struct{id=2}", getPayload("/topology-test/debezium-score-messages/score2.json"));
        kafkaTemplate.send(SCORE_TOPIC, "Struct{id=3}", getPayload("/topology-test/debezium-score-messages/score3.json"));
        kafkaTemplate.send(SCORE_TOPIC, "Struct{id=4}", getPayload("/topology-test/debezium-score-messages/score4.json"));
        kafkaTemplate.send(SCORE_TOPIC, "Struct{id=5}", getPayload("/topology-test/debezium-score-messages/score5.json"));
        kafkaTemplate.send(SCORE_TOPIC, "Struct{id=6}", getPayload("/topology-test/debezium-score-messages/score6.json"));
        kafkaTemplate.send(SCORE_TOPIC, "Struct{id=7}", getPayload("/topology-test/debezium-score-messages/score7.json"));

        kafkaTemplate.send(SCORE_TOPIC, "Struct{id=8}", getPayload("/topology-test/debezium-score-messages/score8.json"));
        kafkaTemplate.send(SCORE_TOPIC, "Struct{id=9}", getPayload("/topology-test/debezium-score-messages/score9.json"));
        kafkaTemplate.send(SCORE_TOPIC, "Struct{id=10}", getPayload("/topology-test/debezium-score-messages/score10.json"));
        kafkaTemplate.send(SCORE_TOPIC, "Struct{id=11}", getPayload("/topology-test/debezium-score-messages/score11.json"));

        kafkaTemplate.flush();

        // Thread.sleep(10000L);

        // Verify Game Rekeys
        final ConsumerRecords<Long, String> gameRekeyRecords = KafkaTestUtils.getRecords(gameRekeyTopicConsumer);
        Assertions.assertEquals(2, gameRekeyRecords.count());

        final Iterator<ConsumerRecord<Long, String>> gameRekeyRecordsIterable = gameRekeyRecords.records(GAME_REKEY_TOPIC).iterator();
        final ConsumerRecord<Long, String> gameRekeyRecord0 = gameRekeyRecordsIterable.next();
        Assertions.assertEquals(1L, gameRekeyRecord0.key());
        JSONAssert.assertEquals(getPayload("/topology-test/game-rekeys/message1.json"), gameRekeyRecord0.value(), false);

        final ConsumerRecord<Long, String> gameRekeyRecord1 = gameRekeyRecordsIterable.next();
        Assertions.assertEquals(2L, gameRekeyRecord1.key());
        JSONAssert.assertEquals(getPayload("/topology-test/game-rekeys/message2.json"), gameRekeyRecord1.value(), false);

        // Verify Score Rekeys
        final ConsumerRecords<Long, String> scoreRekeyRecords = KafkaTestUtils.getRecords(scoreRekeyTopicConsumer);
        Assertions.assertEquals(11, scoreRekeyRecords.count());

        final Iterator<ConsumerRecord<Long, String>> scoreRekeyRecordsIterable = scoreRekeyRecords.records(SCORE_REKEY_TOPIC).iterator();
        final ConsumerRecord<Long, String> scoreRekeyRecord0 = scoreRekeyRecordsIterable.next();
        Assertions.assertEquals(1L, scoreRekeyRecord0.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message1.json"), scoreRekeyRecord0.value(), false);

        final ConsumerRecord<Long, String> scoreRekeyRecord1 = scoreRekeyRecordsIterable.next();
        Assertions.assertEquals(1L, scoreRekeyRecord1.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message2.json"), scoreRekeyRecord1.value(), false);

        final ConsumerRecord<Long, String> scoreRekeyRecord2 = scoreRekeyRecordsIterable.next();
        Assertions.assertEquals(1L, scoreRekeyRecord2.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message3.json"), scoreRekeyRecord2.value(), false);

        final ConsumerRecord<Long, String> scoreRekeyRecord3 = scoreRekeyRecordsIterable.next();
        Assertions.assertEquals(1L, scoreRekeyRecord3.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message4.json"), scoreRekeyRecord3.value(), false);

        final ConsumerRecord<Long, String> scoreRekeyRecord4 = scoreRekeyRecordsIterable.next();
        Assertions.assertEquals(1L, scoreRekeyRecord4.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message5.json"), scoreRekeyRecord4.value(), false);

        final ConsumerRecord<Long, String> scoreRekeyRecord5 = scoreRekeyRecordsIterable.next();
        Assertions.assertEquals(1L, scoreRekeyRecord5.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message6.json"), scoreRekeyRecord5.value(), false);

        final ConsumerRecord<Long, String> scoreRekeyRecord6 = scoreRekeyRecordsIterable.next();
        Assertions.assertEquals(1L, scoreRekeyRecord6.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message7.json"), scoreRekeyRecord6.value(), false);

        final ConsumerRecord<Long, String> scoreRekeyRecord7 = scoreRekeyRecordsIterable.next();
        Assertions.assertEquals(2L, scoreRekeyRecord7.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message8.json"), scoreRekeyRecord7.value(), false);

        final ConsumerRecord<Long, String> scoreRekeyRecord8 = scoreRekeyRecordsIterable.next();
        Assertions.assertEquals(2L, scoreRekeyRecord8.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message9.json"), scoreRekeyRecord8.value(), false);

        final ConsumerRecord<Long, String> scoreRekeyRecord9 = scoreRekeyRecordsIterable.next();
        Assertions.assertEquals(2L, scoreRekeyRecord9.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message10.json"), scoreRekeyRecord9.value(), false);

        final ConsumerRecord<Long, String> scoreRekeyRecord10 = scoreRekeyRecordsIterable.next();
        Assertions.assertEquals(2L, scoreRekeyRecord10.key());
        JSONAssert.assertEquals(getPayload("/topology-test/score-rekeys/message11.json"), scoreRekeyRecord10.value(), false);

        // Verify joins
        final ConsumerRecords<Long, String> joinRecords = KafkaTestUtils.getRecords(gameAndScoreJoinConsumer);
        Assertions.assertEquals(11, joinRecords.count());

        System.out.println("Hello!");
        System.out.println("Your consumed join records are: " + joinRecords.count());
        joinRecords.forEach(System.out::println);
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
