package io.github.andygabler.kafkalearning.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProducerApp {

    private static final Logger LOGGER = Logger.getLogger("ProducerApp");

    public static void main(String[] args) {
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        LOGGER.info(() -> "Started Producer app. Attempting to connect.");

        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(properties);
            LOGGER.info(() -> "Producer connected to broker.");

            final Scanner scanner = new Scanner(System.in);
            boolean running = true;
            while (running) {
                System.out.println("Enter option.");
                System.out.println("\t1. Send message. (enter \"1 <message>\")");
                System.out.println("\t2. Quit");

                final String userInput = scanner.nextLine();
                if (userInput.indexOf("1 ") == 0) {
                   final String message = userInput.substring(2);
                   final ProducerRecord<String, String> record = new ProducerRecord<>(
                       "my-kafka-topic.message", // Topic
                       null, // null key for the message
                       message // Payload
                   );
                   LOGGER.info(() -> "Sending payload of \"" + message + "\" to topic.");
                   // TODO - Handle better. This takes a long time
                   // TODO technically, I could do linger.ms or I could batch
                   producer.send(record);
                } else if (userInput.indexOf("2") == 0) {
                    running = false;
                    LOGGER.info(() -> "Terminate requested.");
                }
            }
        } catch (Exception exception) {
            LOGGER.log(Level.SEVERE, "Exception caught.", exception);
        } finally {
            if (producer != null) {
                LOGGER.info(() -> "Closing producer.");
                producer.flush();
                producer.close();
            }
        }
    }
}
