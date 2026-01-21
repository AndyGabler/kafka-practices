package io.github.andygabler.nfl.result.consumer.kafka;

import io.github.andygabler.nfl.result.consumer.jpa.GameResultRepository;
import io.github.andygabler.nfl.result.consumer.jpa.model.GameResultEntity;
import io.github.andygabler.nfl.result.consumer.kafka.model.ConsumedGameResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

    @Autowired
    private KafkaConsumer<String, ConsumedGameResult> kafkaConsumer;

    @Autowired
    private GameResultRepository repository;

    public void updateGameResults() {
        final ConsumerRecords<String, ConsumedGameResult> kafkaRecords = kafkaConsumer.poll(Duration.ofMillis(500));
        
        kafkaRecords.forEach(this::saveRecord);
    }

    private void saveRecord(ConsumerRecord<String, ConsumedGameResult> record) {
        final ConsumedGameResult consumedGameResult = record.value();

        final GameResultEntity gameResult = new GameResultEntity();
        gameResult.setHomeTeam(consumedGameResult.getHomeTeam());
        gameResult.setHomeTeamScore(consumedGameResult.getHomeTeamScore());
        gameResult.setVisitingTeam(consumedGameResult.getVisitingTeam());
        gameResult.setVisitingTeamScore(consumedGameResult.getVisitingTeamScore());
        repository.save(gameResult);
    }
}
