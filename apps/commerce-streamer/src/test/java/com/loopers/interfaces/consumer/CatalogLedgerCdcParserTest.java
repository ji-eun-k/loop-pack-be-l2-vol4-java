package com.loopers.interfaces.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.domain.catalog.CatalogLedgerEvent;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CatalogLedgerCdcParserTest {

    private final CatalogLedgerCdcParser parser = new CatalogLedgerCdcParser(new ObjectMapper());

    @Test
    void parsesDebeziumUnwrappedRow() {
        CatalogLedgerEvent event = parser.parse("""
            {
              "event_id":"event-1",
              "event_type":"ProductLikedEvent",
              "occurred_at":1784160000000,
              "received_at":1784160001000,
              "payload":{"productId":1001},
              "source_topic":"catalog-events-v1",
              "source_partition":1,
              "source_offset":20,
              "event_version":1
            }
            """);

        assertThat(event.eventId()).isEqualTo("event-1");
        assertThat(event.eventType()).isEqualTo("ProductLikedEvent");
        assertThat(event.payload()).isEqualTo("{\"productId\":1001}");
        assertThat(event.sourceOffset()).isEqualTo(20L);
    }
}