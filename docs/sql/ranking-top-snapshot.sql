CREATE TABLE ranking_top_snapshot (
    ranking_date DATE NOT NULL,
    rank_position INT NOT NULL,
    product_id BIGINT NOT NULL,
    score DOUBLE NOT NULL,
    snapshotted_at DATETIME(6) NOT NULL,
    PRIMARY KEY (ranking_date, rank_position),
    INDEX idx_ranking_snapshot_date_product (ranking_date, product_id)
);
