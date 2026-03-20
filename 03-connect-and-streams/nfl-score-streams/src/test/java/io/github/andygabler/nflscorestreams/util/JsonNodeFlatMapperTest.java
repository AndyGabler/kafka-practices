package io.github.andygabler.nflscorestreams.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class JsonNodeFlatMapperTest {
    private final JsonNodeFlatMapper<String> objectUnderTest = new JsonNodeFlatMapper<>();

    @Test
    public void testApply_happyPath() {
        final String json = "{\"myJson\":\"hello!\"}";

        final List<KeyValue<String, JsonNode>> result = objectUnderTest.apply("key", json);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("key", result.get(0).key);
        Assertions.assertNotNull(result.get(0).value);
        Assertions.assertNotNull(result.get(0).value.get("myJson"));
        Assertions.assertEquals("hello!", result.get(0).value.get("myJson").textValue());
    }

    @Test
    public void testApply_null() {
        final List<KeyValue<String, JsonNode>> result = objectUnderTest.apply("key", null);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testApply_badJson() {
        final List<KeyValue<String, JsonNode>> result = objectUnderTest.apply("key", "not JSON");
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testApply_jsonArray() {
        final String json = "[{\"myJson\":\"hello!\"}, {\"myJson\":\"goodbye!\"}]";

        final List<KeyValue<String, JsonNode>> result = objectUnderTest.apply("key", json);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("key", result.get(0).key);
        Assertions.assertNotNull(result.get(0).value);
        Assertions.assertTrue(result.get(0).value.isArray());
    }
}
