package io.github.andygabler.nfl.result.consumer;

import io.github.andygabler.nfl.result.consumer.jpa.GameResultRepository;
import io.github.andygabler.nfl.result.consumer.jpa.model.GameResultEntity;
import io.github.andygabler.nfl.result.consumer.kafka.ConsumeGameResultService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class GameResultLookupController {

    private static final Logger LOGGER = LoggerFactory.getLogger(GameResultLookupController.class);

    @Autowired
    private GameResultRepository repository;

    @Autowired
    private ConsumeGameResultService consumeGameResultService;

    @GetMapping("/gameResults")
    public List<GameResultEntity> getGameResults() {
        LOGGER.info("Attempting to update game results.");
        consumeGameResultService.updateGameResults();

        LOGGER.info("Game results updated. Querying.");
        return repository.findAll();
    }
}
