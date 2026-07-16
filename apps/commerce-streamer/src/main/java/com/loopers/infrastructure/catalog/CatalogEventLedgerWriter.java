package com.loopers.infrastructure.catalog;

import com.loopers.domain.catalog.CatalogLedgerEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class CatalogEventLedgerWriter {

    private final JdbcTemplate jdbcTemplate;

    @Transactional
    public void appendAll(List<CatalogLedgerEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        jdbcTemplate.batchUpdate(
            """
                INSERT INTO catalog_event_ledger
                    (event_id, event_type, occurred_at, received_at, payload,
                     source_topic, source_partition, source_offset, event_version)
                VALUES (?, ?, ?, ?, CAST(? AS JSON), ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE id = id
                """,
            events,
            events.size(),
            (statement, event) -> {
                statement.setString(1, event.eventId());
                statement.setString(2, event.eventType());
                statement.setTimestamp(3, Timestamp.from(event.occurredAt()));
                statement.setTimestamp(4, Timestamp.from(event.receivedAt()));
                statement.setString(5, event.payload());
                statement.setString(6, event.sourceTopic());
                statement.setInt(7, event.sourcePartition());
                statement.setLong(8, event.sourceOffset());
                statement.setInt(9, event.eventVersion());
            }
        );
    }
}
