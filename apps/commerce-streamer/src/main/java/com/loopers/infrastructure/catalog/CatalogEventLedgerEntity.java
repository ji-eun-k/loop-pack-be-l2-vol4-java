package com.loopers.infrastructure.catalog;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * catalog_event_ledger 테이블 매핑 엔티티. event_id에 UNIQUE 제약을 걸어 동일 이벤트의
 * 중복 적재를 DB 레벨에서도 막는다(애플리케이션 멱등 체크의 이중 안전장치).
 */
@Entity(name = "CatalogEventLedger")
@Table(
    name = "catalog_event_ledger",
    uniqueConstraints = {
        @UniqueConstraint(name = "uk_catalog_ledger_event_id", columnNames = "event_id")
    },
    indexes = {
        @Index(name = "idx_catalog_ledger_occurred_at", columnList = "occurred_at"),
        @Index(name = "idx_catalog_ledger_type_occurred", columnList = "event_type, occurred_at")
    }
)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class CatalogEventLedgerEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, updatable = false, length = 100)
    private String eventId;

    @Column(name = "event_type", nullable = false, updatable = false, length = 100)
    private String eventType;

    @Column(name = "occurred_at", nullable = false, updatable = false)
    private Instant occurredAt;

    @Column(name = "received_at", nullable = false, updatable = false)
    private Instant receivedAt;

    @Column(name = "payload", nullable = false, updatable = false, columnDefinition = "JSON")
    private String payload;

    @Column(name = "source_topic", nullable = false, updatable = false)
    private String sourceTopic;

    @Column(name = "source_partition", nullable = false, updatable = false)
    private Integer sourcePartition;

    @Column(name = "source_offset", nullable = false, updatable = false)
    private Long sourceOffset;

    @Column(name = "event_version", nullable = false, updatable = false)
    private Integer eventVersion;
}
