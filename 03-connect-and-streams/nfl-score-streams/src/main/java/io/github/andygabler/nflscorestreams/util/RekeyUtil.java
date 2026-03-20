package io.github.andygabler.nflscorestreams.util;

public class RekeyUtil {
    public static long parseDebeziumKey(String debeziumKey) {
        return Long.parseLong(debeziumKey.replaceAll(".*id=(\\d+).*", "$1"));
    }
}
