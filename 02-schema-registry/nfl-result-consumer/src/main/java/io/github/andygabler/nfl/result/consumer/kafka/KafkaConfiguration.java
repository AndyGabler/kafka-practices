package io.github.andygabler.nfl.result.consumer.kafka;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.github.andygabler.nfl.result.consumer.kafka.model.ConsumedGameResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

@Configuration
public class KafkaConfiguration {

    private static final String CONSUMER_GROUP_NAME_BASE = "nfl-result-consumer-";

    @Bean
    public KafkaConsumer<String, ConsumedGameResult> kafkaConsumer() {
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        // Since we're using H2 database, consume from beginning each time
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME_BASE + UUID.randomUUID());
        consumerProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        /*
         * Note, strong typing is not recommended as this does break schema evolution a bit but this is for
         * demonstration purposes.
         */
        consumerProperties.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, ConsumedGameResult.class);

        final KafkaConsumer<String, ConsumedGameResult> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList("nfl.game.result.v3"));

        return consumer;
    }
}
