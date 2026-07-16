package com.loopers.interfaces.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.ranking.RankingEventDateResolver;
import com.loopers.domain.catalog.CatalogLedgerEvent;
import com.loopers.infrastructure.catalog.CatalogEventLedgerWriter;
import com.loopers.confg.kafka.KafkaConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class CatalogEventLedgerConsumer {

    private final ObjectMapper objectMapper;
    private final CatalogEventLedgerWriter ledgerWriter;
    private final RankingEventDateResolver eventResolver;
    private final DlqPublisher dlqPublisher;
    private final Clock clock;

    @KafkaListener(
        topics = {"catalog-events-v1", "catalog-view-events-v1"},
        groupId = "loopers-catalog-event-ledger-consumer",
        containerFactory = KafkaConfig.BATCH_LISTENER
    )
    public void consume(List<ConsumerRecord<Object, Object>> records, Acknowledgment acknowledgment) {
        List<CatalogLedgerEvent> events = new ArrayList<>();
        for (ConsumerRecord<Object, Object> record : records) {
            try {
                events.add(toLedgerEvent(record));
            } catch (RuntimeException exception) {
                log.warn("[CATALOG_LEDGER] 원본 이벤트 정규화 실패 — topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset(), exception);
                dlqPublisher.sendToDlq(record, exception);
            }
        }
        ledgerWriter.appendAll(events);
        acknowledgment.acknowledge();
    }

    private CatalogLedgerEvent toLedgerEvent(ConsumerRecord<Object, Object> record) {
        String payload = record.value().toString();
        Map<String, Object> body;
        try {
            body = objectMapper.readValue(payload, new TypeReference<>() {});
        } catch (Exception exception) {
            throw new IllegalArgumentException("catalog event payload must be JSON", exception);
        }

        boolean viewEvent = "catalog-view-events-v1".equals(record.topic());
        String eventId = eventResolver.eventId(
            viewEvent ? stringValue(body.get("eventId")) : header(record, "X-Event-Id"), record
        );
        String eventType = viewEvent ? "ProductViewedEvent" : header(record, "X-Event-Type");
        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException("eventType is required");
        }
        String occurredAtValue = viewEvent
            ? stringValue(body.get("occurredAt"))
            : header(record, "X-Event-Occurred-At");

        return new CatalogLedgerEvent(
            eventId,
            eventType,
            parseInstant(occurredAtValue, record),
            clock.instant(),
            payload,
            record.topic(),
            record.partition(),
            record.offset(),
            parseVersion(header(record, "X-Event-Version"))
        );
    }

    private Instant parseInstant(String value, ConsumerRecord<?, ?> record) {
        if (value != null && !value.isBlank()) {
            try {
                return Instant.parse(value);
            } catch (DateTimeParseException ignored) {
                try {
                    return OffsetDateTime.parse(value).toInstant();
                } catch (DateTimeParseException ignoredAgain) {
                    try {
                        return ZonedDateTime.parse(value).toInstant();
                    } catch (DateTimeParseException ignoredOnceMore) {
                        // Kafka timestamp fallback
                    }
                }
            }
        }
        return record.timestamp() >= 0 ? Instant.ofEpochMilli(record.timestamp()) : clock.instant();
    }

    private int parseVersion(String value) {
        if (value == null || value.isBlank()) {
            return 1;
        }
        return Integer.parseInt(value);
    }

    private String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private String stringValue(Object value) {
        return value == null ? null : value.toString();
    }
}
