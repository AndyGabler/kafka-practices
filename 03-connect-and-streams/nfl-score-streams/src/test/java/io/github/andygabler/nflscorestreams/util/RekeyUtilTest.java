package io.github.andygabler.nflscorestreams.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RekeyUtilTest {

    @Test
    public void testParseDebeziumKey() {
        Assertions.assertEquals(1L, RekeyUtil.parseDebeziumKey("Struct{id=1}"));
        Assertions.assertEquals(213L, RekeyUtil.parseDebeziumKey("Struct{id=213}"));
    }
}
