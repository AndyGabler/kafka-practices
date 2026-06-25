package io.github.andygabler.bigfoot.consumer.model;

import lombok.Data;

@Data
public class BigfootSighting {
    private String spotter;
    private float latitude;
    private float longitude;
    private String sightingType;
}
