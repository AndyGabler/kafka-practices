package io.github.andygabler.nfl.result.producer.gameresult;

import io.github.andygabler.nfl.result.producer.gameresult.model.GameResult;
import io.github.andygabler.nfl.result.producer.gameresult.model.Team;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class GameResultService {

    private static final Logger LOGGER = LoggerFactory.getLogger(GameResultService.class);
    public static final String TOPIC_NAME = "nfl.game.result.v3";

    @Autowired
    private KafkaProducer<String, GameResult> kafkaProducer;

    /**
     * Post the result for a game to a Kafka topic.
     *
     * @param result The result to post
     */
    public void postResult(GameResultFormDTO result) {
        final GameResult gameResult = new GameResult();
        gameResult.setHomeTeam(Team.valueOf(result.getHomeTeam()));
        gameResult.setVisitingTeam(Team.valueOf(result.getVisitingTeam()));
        gameResult.setHomeTeamScore(result.getHomeTeamScore());
        gameResult.setVisitingTeamScore(result.getVisitingTeamScore());
        gameResult.setDatePlayed(result.getDatePlayed());

        final String key = gameResult.getVisitingTeam() + " @ " + gameResult.getHomeTeam();

        LOGGER.info("Sending result of " + key + " to Kafka topic.");

        final ProducerRecord<String, GameResult> record = new ProducerRecord<>(TOPIC_NAME, key, gameResult);
        kafkaProducer.send(record);
    }
}
