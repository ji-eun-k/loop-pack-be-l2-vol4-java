package com.loopers.application.ranking;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;

@Component
@RequiredArgsConstructor
public class RankingEventDateResolver {

    private final Clock clock;

    public LocalDate resolve(String occurredAt, ConsumerRecord<?, ?> record) {
        if (occurredAt != null && !occurredAt.isBlank()) {
            try {
                return ZonedDateTime.parse(occurredAt).withZoneSameInstant(clock.getZone()).toLocalDate();
            } catch (DateTimeParseException ignored) {
                try {
                    return OffsetDateTime.parse(occurredAt).atZoneSameInstant(clock.getZone()).toLocalDate();
                } catch (DateTimeParseException ignoredAgain) {
                    try {
                        return Instant.parse(occurredAt).atZone(clock.getZone()).toLocalDate();
                    } catch (DateTimeParseException ignoredOnceMore) {
                        // 구버전/잘못된 헤더는 Kafka timestamp로 보정한다.
                    }
                }
            }
        }

        if (record.timestamp() >= 0) {
            return Instant.ofEpochMilli(record.timestamp()).atZone(clock.getZone()).toLocalDate();
        }
        return LocalDate.now(clock);
    }

    public String eventId(String eventId, ConsumerRecord<?, ?> record) {
        if (eventId != null && !eventId.isBlank()) {
            return eventId;
        }
        return record.topic() + ":" + record.partition() + ":" + record.offset();
    }
}
