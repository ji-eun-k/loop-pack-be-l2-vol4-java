package com.loopers.infrastructure.catalog;

import org.springframework.data.jpa.repository.JpaRepository;

public interface EventHandledJpaRepository extends JpaRepository<EventHandledEntity, Long> {
    boolean existsByEventId(String eventId);
}
