package io.github.andygabler.nfl.result.producer.gameresult;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class GameResultService {

    @Autowired
    private KafkaProducer<String, GameResult> kafkaProducer;

    public void postResult(GameResult result) {
        final String key = result.getVisitingTeam() + " @ " + result.getHomeTeam();

        final ProducerRecord<String, GameResult> record = new ProducerRecord<>(key, result);
        kafkaProducer.send(record);
    }
}
