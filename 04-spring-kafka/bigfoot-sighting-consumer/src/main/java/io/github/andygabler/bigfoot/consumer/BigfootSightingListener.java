package io.github.andygabler.bigfoot.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class BigfootSightingListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(BigfootSightingListener.class);

    @KafkaListener(id = "bigfoot-sighting-consumer", topics = {"bigfoot.sighting"})
    public void handleMessage(String sightingJson) {
        LOGGER.info("Consumed new record JSON: " + sightingJson);
    }
}
