package io.github.andygabler.nflscorerestapi.repository;

import io.github.andygabler.nflscorerestapi.model.FootballGame;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FootballGameRepository extends JpaRepository<FootballGame, Long> {
}
