package io.github.andygabler.nfl.result.consumer.kafka;

import io.github.andygabler.nfl.result.consumer.jpa.GameResultRepository;
import io.github.andygabler.nfl.result.consumer.jpa.model.GameResultEntity;
import io.github.andygabler.nfl.result.consumer.kafka.model.ConsumedGameResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Service to transport GameResult records from Kafka topic to an H2 in-memory database.
 *
 * A few elephants in the room to address here.
 *  1. In a real environment, we would use a Sink connector instead of doing this ourselves.
 *  2. Spring + Kafka Streams is a better way to do this. But the reason this is done is to learn this stuff and
 *     handle bad records in Schema Registry and see how that looks
 */
@Service
public class ConsumeGameResultService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeGameResultService.class);

    @Autowired
    private KafkaConsumer<String, ConsumedGameResult> kafkaConsumer;

    @Autowired
    private GameResultRepository repository;

    /**
     * Attempt to read Kafka topic to update the results in the H2 in-memory database.
     */
    public void updateGameResults() {
        ConsumerRecords<String, ConsumedGameResult> kafkaRecords;
        try {
            LOGGER.info("Attempting to update game results. Starting initial poll.");
            /*
             * This initial call to poll may not do anything. Since this is from a request thread, it may take multiple
             * calls to get results. This is because the first call might need to do some other tasks.
             */
            kafkaRecords = kafkaConsumer.poll(Duration.ofMillis(500));
        } catch (Exception exception) {
            LOGGER.error("Failure in polling for records. Committing offset to skip.", exception);
            kafkaConsumer.seekToEnd(kafkaConsumer.assignment());
            throw exception;
        }
        kafkaRecords.forEach(this::saveRecord);
    }

    /**
     * Save a result to the database from a Kafka record.
     *
     * @param record The Kafka record to save to the database
     */
    private void saveRecord(ConsumerRecord<String, ConsumedGameResult> record) {
        LOGGER.info(
            "Found new message.\n" +
            "\tTopic = " +
            record.topic() +
            "\n" +
            "\tPartition = " +
            record.partition() +
            "\n" +
            "\tKey = " +
            record.key()
        );
        final ConsumedGameResult consumedGameResult = record.value();

        final GameResultEntity gameResult = new GameResultEntity();
        gameResult.setHomeTeam(consumedGameResult.getHomeTeam());
        gameResult.setHomeTeamScore(consumedGameResult.getHomeTeamScore());
        gameResult.setVisitingTeam(consumedGameResult.getVisitingTeam());
        gameResult.setVisitingTeamScore(consumedGameResult.getVisitingTeamScore());
        repository.save(gameResult);
    }
}
