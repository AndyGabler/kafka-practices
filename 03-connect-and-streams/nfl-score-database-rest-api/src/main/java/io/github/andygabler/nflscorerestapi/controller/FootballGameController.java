package io.github.andygabler.nflscorerestapi.controller;

import io.github.andygabler.nflscorerestapi.model.FootballGame;
import io.github.andygabler.nflscorerestapi.repository.FootballGameRepository;
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
@RequestMapping("/game")
public class FootballGameController {

    @Autowired
    private FootballGameRepository repository;

    @GetMapping
    public Page<FootballGame> getFootballGame(Pageable pageable) {
        return repository.findAll(pageable);
    }

    @GetMapping("/{id}")
    public List<FootballGame> getFootballGame(
        @PathVariable("id")
        Long id
    ) {
        return repository.findById(id).map(Collections::singletonList).orElse(Collections.emptyList());
    }

    @PostMapping
    public FootballGame postFootballGame(
        @RequestBody
        FootballGame game
    ) {
        Objects.requireNonNull(game.getHomeTeam(), "homeTeam must not be null");
        Objects.requireNonNull(game.getVisitingTeam(), "visitingTeam must not be null");
        Objects.requireNonNull(game.getMatchDate(), "matchDate must not be null");
        return repository.save(game);
    }
}
