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

/**
 * 이벤트가 어느 날짜의 랭킹에 반영돼야 하는지 결정한다.
 * 랭킹은 날짜별 Redis 키(ranking:yyyy-MM-dd)로 분리 저장되므로, occurredAt 파싱에 실패하면
 * Kafka record의 timestamp로, 그마저 없으면 현재 시각으로 순차 폴백한다.
 */
@Component
@RequiredArgsConstructor
public class RankingEventDateResolver {

    private final Clock clock;

    public LocalDate resolve(String occurredAt, ConsumerRecord<?, ?> record) {
        // ZonedDateTime → OffsetDateTime → Instant 순으로 파싱을 시도한다 (프로듀서 버전에 따라 포맷이 다를 수 있음).
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
        // payload에 eventId가 없는 경우 topic:partition:offset을 대체 식별자로 사용해 멱등 처리를 유지한다.
        return record.topic() + ":" + record.partition() + ":" + record.offset();
    }
}
