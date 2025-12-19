package io.github.andygabler.nfl.result.producer.schema;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.github.andygabler.nfl.result.producer.gameresult.GameResultService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@Controller
public class SchemaController {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaController.class);

    @Autowired
    private SchemaRegistryClient schemaRegistryClient;

    @GetMapping("/schemaInfo")
    public ModelAndView schemaInfo(String userMessage) {
        LOGGER.info("Schema form loaded.");

        String schemaText = "";
        try {
            schemaText = schemaText();
        } catch (Exception exception) {
            userMessage = exception.toString();
            LOGGER.error("Unable to load JSON schema.", exception);
        }

        final ModelAndView modelAndView = new ModelAndView("schemaForm");
        modelAndView.addObject("userMessage", userMessage);
        modelAndView.addObject("jsonSchemaText", schemaText);
        return modelAndView;
    }

    @PostMapping("/schemaInfo")
    public ModelAndView submitSchema() {
        LOGGER.info("Attempting to submit schema.");
        try {
            final String schemaDefinition = schemaText();
            LOGGER.info("Schema content to submit: \n" + schemaDefinition);

            final JsonSchema jsonSchema = new JsonSchema(schemaDefinition);
            final String subject = GameResultService.TOPIC_NAME + "-value";
            final int schemaId = schemaRegistryClient.register(subject, jsonSchema);

            LOGGER.info("Schema submitted with ID " + schemaId);
            return schemaInfo("Schema submitted.");
        } catch (Exception exception) {
            LOGGER.error("Unable to submit schema.", exception);
            return schemaInfo(exception.toString());
        }
    }

    private String schemaText() throws IOException {
        return new String(
            SchemaController
                .class
                .getResource("/kafka-schema-v1.json")
                .openStream()
                .readAllBytes(),
            StandardCharsets.UTF_8
        );
    }
}
