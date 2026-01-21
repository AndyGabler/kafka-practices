package io.github.andygabler.nfl.result.consumer;

import io.github.andygabler.nfl.result.consumer.jpa.GameResultRepository;
import io.github.andygabler.nfl.result.consumer.jpa.model.GameResultEntity;
import io.github.andygabler.nfl.result.consumer.kafka.ConsumeGameResultService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class GameResultLookupController {

    @Autowired
    private GameResultRepository repository;

    @Autowired
    private ConsumeGameResultService consumeGameResultService;

    @GetMapping("/gameResults")
    public List<GameResultEntity> getGameResults() {
        consumeGameResultService.updateGameResults();
        return repository.findAll();
    }
}
