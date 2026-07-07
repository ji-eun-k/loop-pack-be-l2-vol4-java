package com.loopers.infrastructure.queue;

public final class QueueLuaScripts {

    // KEYS[1]: "queue:waiting"
    // ARGV[1]: userId (string)
    // ARGV[2]: score (Unix timestamp ms, string)
    // returns: {rank(0-based), totalCount}
    public static final String ENTER = """
            redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
            local rank  = redis.call('ZRANK', KEYS[1], ARGV[1])
            local total = redis.call('ZCARD', KEYS[1])
            return {rank, total}
            """;

    // KEYS[1]: "queue:waiting"
    // ARGV[1]: userId (string)
    // returns: {-1} if not in queue, {rank(0-based), totalWaiting} if waiting
    public static final String POSITION = """
            local rank = redis.call('ZRANK', KEYS[1], ARGV[1])
            if rank == false then return {-1} end
            local total = redis.call('ZCARD', KEYS[1])
            return {rank, total}
            """;

    private QueueLuaScripts() {}
}
