package io.github.andygabler.bigfoot.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.andygabler.bigfoot.producer.model.BigfootSighting;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
@EnableScheduling
public class BigfootSightingProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(BigfootSightingProducer.class);

    private static final String[] NAME_BANK = {
        "Angel",
        "Bennie",
        "Bertie",
        "Billy",
        "Caroll",
        "Casey",
        "Chris",
        "Cody",
        "Devin",
        "Demy",
        "Dorian",
        "Finn",
        "Jordan",
        "Justice",
        "Morgan",
        "Sam",
        "Terry",
        "Westley"
    };

    private static final String[] SIGHTING_TYPES = {
        "Grainy Camera Shot",
        "Shaky Video",
        "Unintelligible Audio Recording",
        "Ambiguous Audio Recording",
        "Hearsay",
        "Unreliable Witness",
        "Fabricated Video",
        "Low-Resolution Satellite Image"
    };

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @SneakyThrows
    @Scheduled(fixedDelay = 5000L)
    public void sightBigfoot() {
        final Random random = new Random();

        final float latitude = random.nextFloat(-90, 90);
        final float longitude = random.nextFloat(-180, 180);
        final String name = NAME_BANK[random.nextInt(NAME_BANK.length)] + " " + NAME_BANK[random.nextInt(NAME_BANK.length)];
        final String sightingType = SIGHTING_TYPES[random.nextInt(SIGHTING_TYPES.length)];

        final String key = "(" + latitude + ", " + longitude + ")";

        final BigfootSighting sighting = new BigfootSighting();
        sighting.setLongitude(longitude);
        sighting.setLatitude(latitude);
        sighting.setSpotter(name);
        sighting.setSightingType(sightingType);

        final String sightingJson = new ObjectMapper().writeValueAsString(sighting);
        kafkaTemplate.send("bigfoot.sighting", key, sightingJson);
        LOGGER.info("New sighting at " + key + " just dropped! Better consume the topic to check it out!");
    }
}
