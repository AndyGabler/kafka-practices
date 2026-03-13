package io.github.andygabler.nflscorerestapi.repository;

import io.github.andygabler.nflscorerestapi.model.GameScore;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface GameScoreRepository extends JpaRepository<GameScore, Long> {
}
