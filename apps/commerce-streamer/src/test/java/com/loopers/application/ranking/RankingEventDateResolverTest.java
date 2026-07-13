package com.loopers.application.ranking;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

class RankingEventDateResolverTest {

    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");
    private final RankingEventDateResolver resolver = new RankingEventDateResolver(
        Clock.fixed(Instant.parse("2026-07-13T01:00:00Z"), SEOUL)
    );

    @Test
    void resolvesDateFromOccurredAt_insteadOfProcessingDate() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 1, 42L, "key", "value");

        LocalDate date = resolver.resolve("2026-07-12T23:59:59+09:00", record);

        assertThat(date).isEqualTo(LocalDate.of(2026, 7, 12));
    }

    @Test
    void createsStableFallbackEventIdFromKafkaPosition() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 1, 42L, "key", "value");

        assertThat(resolver.eventId(null, record)).isEqualTo("topic:1:42");
    }
}
