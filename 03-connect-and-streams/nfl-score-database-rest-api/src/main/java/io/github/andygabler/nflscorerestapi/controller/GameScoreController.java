package io.github.andygabler.nflscorerestapi.controller;

import io.github.andygabler.nflscorerestapi.model.GameScore;
import io.github.andygabler.nflscorerestapi.repository.GameScoreRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@RestController
@RequestMapping("/score")
public class GameScoreController {

    @Autowired
    private GameScoreRepository repository;

    @GetMapping
    public Page<GameScore> getGameScore(Pageable pageable) {
        return repository.findAll(pageable);
    }

    @GetMapping("/{id}")
    public List<GameScore> getGameScore(
        @PathVariable("id")
        Long id
    ) {
        return repository.findById(id).map(Collections::singletonList).orElse(Collections.emptyList());
    }

    @PostMapping
    public GameScore postGameScore(
        @RequestBody
        GameScore score
    ) {
        Objects.requireNonNull(score.getFootballGameId(), "footballGameId must not be null");
        Objects.requireNonNull(score.getTeam(), "team must not be null");
        Objects.requireNonNull(score.getSnapType(), "snapType must not be null");
        Objects.requireNonNull(score.getQuarterback(), "quarterback must not be null");
        Objects.requireNonNull(score.getBallCarrier(), "ballCarrier must not be null");
        return repository.save(score);
    }
}
