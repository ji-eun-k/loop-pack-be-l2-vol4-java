package com.loopers.infrastructure.catalog;

import com.loopers.domain.catalog.CatalogLedgerEvent;
import com.loopers.testcontainers.MySqlTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(MySqlTestContainersConfig.class)
class CatalogEventLedgerWriterIntegrationTest {

    @Autowired private CatalogEventLedgerWriter writer;
    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private DatabaseCleanUp databaseCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
    }

    @Test
    void appendsEventOnceWhenSameEventIsRetried() {
        CatalogLedgerEvent event = new CatalogLedgerEvent(
            "event-1", "ProductViewedEvent", Instant.parse("2026-07-16T00:00:00Z"),
            Instant.parse("2026-07-16T00:00:01Z"), "{\"productId\":1001}",
            "catalog-view-events-v1", 0, 10L, 1
        );

        writer.appendAll(List.of(event));
        writer.appendAll(List.of(event));

        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM catalog_event_ledger", Integer.class);
        assertThat(count).isEqualTo(1);
    }

    @Test
    void appendsBothEventsWhenSourcePositionIsSameButEventIdsAreDifferent() {
        CatalogLedgerEvent first = new CatalogLedgerEvent(
            "event-1", "ProductViewedEvent", Instant.parse("2026-07-16T00:00:00Z"),
            Instant.parse("2026-07-16T00:00:01Z"), "{\"productId\":1001}",
            "catalog-view-events-v1", 0, 10L, 1
        );
        CatalogLedgerEvent second = new CatalogLedgerEvent(
            "event-2", "ProductViewedEvent", Instant.parse("2026-07-16T00:00:00Z"),
            Instant.parse("2026-07-16T00:00:01Z"), "{\"productId\":1002}",
            "catalog-view-events-v1", 0, 10L, 1
        );

        writer.appendAll(List.of(first, second));

        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM catalog_event_ledger", Integer.class);
        assertThat(count).isEqualTo(2);
    }
}