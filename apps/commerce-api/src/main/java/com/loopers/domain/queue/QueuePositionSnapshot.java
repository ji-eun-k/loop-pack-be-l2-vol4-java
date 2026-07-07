package com.loopers.domain.queue;

/**
 * ZSET에서 ZRANK(0-based)와 ZCARD를 Lua로 원자적으로 읽은 스냅샷.
 */
public record QueuePositionSnapshot(long rank, long totalWaiting) {}
