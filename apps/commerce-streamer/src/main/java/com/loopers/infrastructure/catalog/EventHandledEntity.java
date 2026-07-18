package com.loopers.infrastructure.catalog;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.Getter;

import java.time.ZonedDateTime;

/**
 * 이벤트 멱등 처리용 마커 테이블. eventId에 UNIQUE 제약이 있어 동일 이벤트가 두 번 저장되지 않으며,
 * CatalogMetricsProcessor는 저장 전 existsByEventId로 중복 처리를 판단한다.
 */
@Getter
@Entity(name = "EventHandled")
@Table(name = "event_handled")
public class EventHandledEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(nullable = false)
    private String topic;

    @Column(name = "handled_at", nullable = false, updatable = false)
    private ZonedDateTime handledAt;

    protected EventHandledEntity() {}

    public EventHandledEntity(String eventId, String topic) {
        this.eventId = eventId;
        this.topic = topic;
    }

    @PrePersist
    private void prePersist() {
        this.handledAt = ZonedDateTime.now();
    }
}
