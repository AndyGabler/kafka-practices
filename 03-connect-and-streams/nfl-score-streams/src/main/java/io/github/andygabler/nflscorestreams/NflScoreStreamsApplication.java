package io.github.andygabler.nflscorestreams;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class NflScoreStreamsApplication {

    public static void main(String[] args) {
        SpringApplication.run(NflScoreStreamsApplication.class, args);
    }

}
