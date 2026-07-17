package com.loopers.interfaces.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.domain.catalog.CatalogLedgerEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.OffsetDateTime;

/**
 * Debezium 등 CDC 커넥터가 catalog_event_ledger 테이블 변경분을 발행한 Kafka 메시지를 파싱한다.
 * CDC 페이로드는 대개 {"payload": {"after": {...row...}}} 형태로 감싸져 오지만, 커넥터/버전에 따라
 * {"after": {...}}만 오거나 이미 row 자체가 오는 경우까지 방어적으로 처리한다.
 */
@Component
@RequiredArgsConstructor
public class CatalogLedgerCdcParser {

    private final ObjectMapper objectMapper;

    public CatalogLedgerEvent parse(Object value) {
        try {
            JsonNode root = objectMapper.readTree(value.toString());
            JsonNode row = unwrapAfter(root);
            return new CatalogLedgerEvent(
                requiredText(row, "event_id"),
                requiredText(row, "event_type"),
                instant(row.get("occurred_at")),
                instant(row.get("received_at")),
                payload(row.get("payload")),
                requiredText(row, "source_topic"),
                row.path("source_partition").asInt(),
                row.path("source_offset").asLong(),
                row.path("event_version").asInt(1)
            );
        } catch (RuntimeException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalArgumentException("invalid catalog ledger CDC event", exception);
        }
    }

    private JsonNode unwrapAfter(JsonNode root) {
        if (root.path("payload").has("after")) {
            return root.path("payload").path("after");
        }
        if (root.has("after")) {
            return root.path("after");
        }
        return root;
    }

    private String payload(JsonNode node) throws Exception {
        if (node == null || node.isNull()) {
            throw new IllegalArgumentException("payload is required");
        }
        return node.isTextual() ? node.asText() : objectMapper.writeValueAsString(node);
    }

    private String requiredText(JsonNode node, String field) {
        String value = node.path(field).asText(null);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }

    private Instant instant(JsonNode node) {
        if (node == null || node.isNull()) {
            throw new IllegalArgumentException("timestamp is required");
        }
        if (node.isNumber()) {
            long value = node.asLong();
            // Debezium은 컬럼 타입에 따라 밀리초/마이크로초 epoch를 혼용해 내려줄 수 있어
            // 자릿수로 마이크로초 단위를 구분해 나노초로 변환한다.
            if (Math.abs(value) > 100_000_000_000_000L) {
                return Instant.ofEpochSecond(0, value * 1_000L);
            }
            return Instant.ofEpochMilli(value);
        }
        String value = node.asText();
        try {
            return Instant.parse(value);
        } catch (Exception ignored) {
            return OffsetDateTime.parse(value).toInstant();
        }
    }
}