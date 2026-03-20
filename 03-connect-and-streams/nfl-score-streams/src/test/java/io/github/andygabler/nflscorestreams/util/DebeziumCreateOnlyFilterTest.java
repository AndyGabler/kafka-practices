package io.github.andygabler.nflscorestreams.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class DebeziumCreateOnlyFilterTest {
    private final DebeziumCreateOnlyFilter objectUnderTest = new DebeziumCreateOnlyFilter();

    @Test
    public void testPredicate_isCreate() throws IOException {
        final JsonNode inputNode = new ObjectMapper().readTree(
            DebeziumCreateOnlyFilterTest
                .class
                .getResource("/util/debezium-test/debezium-add.json").
                openStream()
        );
        Assertions.assertTrue(objectUnderTest.test("key", inputNode));
    }

    @Test
    public void testPredicate_isDelete() throws IOException {
        final JsonNode inputNode = new ObjectMapper().readTree(
            DebeziumCreateOnlyFilterTest
                .class
                .getResource("/util/debezium-test/debezium-delete.json").
                openStream()
        );
        Assertions.assertFalse(objectUnderTest.test("key", inputNode));
    }
}
