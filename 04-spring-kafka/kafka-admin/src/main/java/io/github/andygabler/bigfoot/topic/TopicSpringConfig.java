package io.github.andygabler.bigfoot.topic;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicSpringConfig {

    @Bean
    public NewTopic bigfootSightingTopic() {
        return TopicBuilder
            .name("bigfoot.sighting")
            .partitions(3)
            // 24 hour retention
            .config(TopicConfig.RETENTION_MS_CONFIG, "86400000")
            .build();
    }
}
