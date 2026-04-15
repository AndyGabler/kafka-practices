package io.github.andygabler.nflscorestreams;

import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;

public class ErrorHandling implements ProductionExceptionHandler {
    // TODO implement me to handle race condition between game rekey and score rekey
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
