CREATE TABLE catalog_event_ledger (
    id BIGINT NOT NULL AUTO_INCREMENT,
    event_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    received_at DATETIME(6) NOT NULL,
    payload JSON NOT NULL,
    source_topic VARCHAR(255) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    event_version INT NOT NULL DEFAULT 1,
    PRIMARY KEY (id),
    UNIQUE KEY uk_catalog_ledger_event_id (event_id),
    INDEX idx_catalog_ledger_occurred_at (occurred_at),
    INDEX idx_catalog_ledger_type_occurred (event_type, occurred_at)
);
