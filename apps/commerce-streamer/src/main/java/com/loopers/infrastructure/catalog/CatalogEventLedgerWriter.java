package com.loopers.infrastructure.catalog;

import com.loopers.domain.catalog.CatalogLedgerEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.List;

/**
 * Kafka에서 소비한 카탈로그 이벤트를 원장 테이블에 배치로 적재한다.
 * 이후 CDC(Change Data Capture)가 이 테이블 변경분을 읽어 product_metrics/랭킹 반영을 트리거한다.
 */
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
            // ON DUPLICATE KEY UPDATE id = id: 실질적으로 no-op 업데이트라 값은 바뀌지 않지만,
            // MySQL에서 INSERT IGNORE 대신 이 패턴을 쓰면 다른 컬럼 위반 에러는 여전히 표면화되면서
            // event_id 중복만 조용히 넘어가는 "upsert 없는 idempotent insert"가 된다.
        );
    }
}
