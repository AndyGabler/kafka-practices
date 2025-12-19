package io.github.andygabler.nfl.result.producer.gameresult;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class GameResultService {

    private static final Logger LOGGER = LoggerFactory.getLogger(GameResultService.class);
    public static final String TOPIC_NAME = "nfl.game.result.v4";

    @Autowired
    private KafkaProducer<String, GameResult> kafkaProducer;

    public void postResult(GameResult result) {
        final String key = result.getVisitingTeam() + " @ " + result.getHomeTeam();

        LOGGER.info("Sending result of " + key + " to Kafka topic.");

        final ProducerRecord<String, GameResult> record = new ProducerRecord<>(TOPIC_NAME, key, result);
        kafkaProducer.send(record);
    }
}
