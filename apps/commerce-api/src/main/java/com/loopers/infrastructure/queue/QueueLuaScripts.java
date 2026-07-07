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
    // KEYS[2]: "queue:active:{userId}" (full key)
    // ARGV[1]: userId (string)
    // returns: {statusCode, rank, total}
    //   statusCode 0=ACTIVE, 1=WAITING, 2=NOT_IN_QUEUE
    //   rank/total are -1 when not applicable
    public static final String POSITION = """
            local isActive = redis.call('EXISTS', KEYS[2])
            if isActive == 1 then return {0, -1, -1} end
            local rank  = redis.call('ZRANK', KEYS[1], ARGV[1])
            local total = redis.call('ZCARD', KEYS[1])
            if rank == false then return {2, -1, -1} end
            return {1, rank, total}
            """;

    private QueueLuaScripts() {}
}
