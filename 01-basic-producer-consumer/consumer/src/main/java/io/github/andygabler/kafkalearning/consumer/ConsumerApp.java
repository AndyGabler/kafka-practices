package io.github.andygabler.kafkalearning.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConsumerApp {

    private static final Logger LOGGER = Logger.getLogger("ConsumerApp");

    private static Properties makeProperties(Scanner inputScanner) {
        System.out.print("Enter consumer name: ");
        final String consumerName = inputScanner.nextLine();

        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerName);

        System.out.print("Enter Auto Reset Config [0 earliest, 1 latest, 2 none]: ");
        final int autoResetChoice = Integer.parseInt(inputScanner.nextLine());
        String autoReset = switch (autoResetChoice) {
            case 0 -> "earliest"; // If no pre-existing offset, start from 0
            case 1 -> "latest"; // If no pre-exiting offset, go to latest in topic
            case 2 -> "none"; // Throw exception if no pre-existing offset
            default -> throw new IllegalArgumentException("Unknown auto reset mapping: " + autoResetChoice);
        };
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoReset);


        LOGGER.info(() -> "Consumer properties determined to be autoReset=\"" + autoReset + "\", groupId=\"" + consumerName + "\".");
        return properties;
    }

    public static void handleMessage(ConsumerRecord<String, String> message) {
        final StringBuilder logBuilder = new StringBuilder();
        logBuilder
            .append("Found new message.\n")
            .append("\tTopic = ")
            .append(message.topic())
            .append("\n")
            .append("\tPartition = ")
            .append(message.partition())
            .append("\n")
            .append("\tKey = ")
            .append(message.key())
            .append("\n")
            .append("\tValue = ")
            .append(message.value())
            .append("\n");

        LOGGER.info(logBuilder::toString);
    }

    public static void main(String[] args) {
        final Scanner inputScanner = new Scanner(System.in);
        final Properties consumerProperties = makeProperties(inputScanner);

        LOGGER.info(() -> "Starting consumer.");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            LOGGER.info(() -> "Subscribing to topic...");
            consumer.subscribe(Collections.singletonList("my-kafka-topic.message"));

            LOGGER.info(() -> "Assignment info: " + consumer.assignment().toString());

            LOGGER.info(() -> "Polling for records...");
            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                if (!records.isEmpty()) {
                    LOGGER.info(() -> "Found " + records.count() + " records.");
                    records.forEach(ConsumerApp::handleMessage);
                }
            }
        } catch (Exception exception) {
            LOGGER.log(Level.SEVERE, "Exception caught.", exception);
        }
    }
}
