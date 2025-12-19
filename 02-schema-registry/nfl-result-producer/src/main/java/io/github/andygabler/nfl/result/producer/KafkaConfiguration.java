package io.github.andygabler.nfl.result.producer;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import io.github.andygabler.nfl.result.producer.gameresult.GameResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaConfiguration {

    @Bean
    public KafkaProducer<String, GameResult> kafkaProducer() {
        /*
         * Here's where this is bad.
         *
         * I highly doubt a singleton Kafka Producer is safe for a bean. There are libraries to manage this for us.
         * That said, this is before I used any Spring library for Kafka connecting.
         *
         * So we're going to go with this for now not because it is correct, but it is what I know I can use without
         * jumping the gun and learning a few Kafka concepts ahead of where I'm at in the Confluent courses.
         */
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA, true);

        /*
         * Here's another spot where I'm purposefully implementing bad practices.
         *
         * I want this to be a demonstration of enforcing the schema on the producer end and showing that the produce
         * FAILS if it does not match the given schema.
         */
        properties.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        return new KafkaProducer<>(properties);
    }

    @Bean
    public SchemaRegistryClient schemaRegistryClient() {
        return new CachedSchemaRegistryClient("http://localhost:8081", 1);
    }
}
