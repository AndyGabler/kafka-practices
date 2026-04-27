package io.github.andygabler.nflscorestreams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class ErrorHandlerConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorHandlerConfiguration.class);


    @Value("${nfl-streams-application.error-handling.retry-delay-millis}")
    private Long retryBackoff;

    @Value("${nfl-streams-application.error-handling.max-attempts}")
    private Long retryMaxAttempts;

    @Bean
    public DefaultErrorHandler errorHandler() {
        return new DefaultErrorHandler((consumerRecord, exception) -> {
            final String recordId = "[" + KafkaUtils.format(consumerRecord) + "]";
            LOGGER.error(recordId + " Failed to process.", exception);
        }, new FixedBackOff(retryBackoff, retryMaxAttempts));
    }
}
