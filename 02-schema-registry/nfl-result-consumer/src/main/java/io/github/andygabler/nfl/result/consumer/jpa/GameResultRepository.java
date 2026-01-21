package io.github.andygabler.nfl.result.consumer.jpa;

import io.github.andygabler.nfl.result.consumer.jpa.model.GameResultEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface GameResultRepository extends JpaRepository<GameResultEntity, Long> {
}
