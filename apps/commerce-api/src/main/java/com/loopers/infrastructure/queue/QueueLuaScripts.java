package com.loopers.infrastructure.queue;

public final class QueueLuaScripts {

    // KEYS[1]: "queue:waiting" — Sorted Set 키
    // ARGV[1]: userId (string) — Sorted Set의 member (값). key가 아님에 주의
    // ARGV[2]: score (Unix timestamp ms, string) — 정렬 기준. 작을수록 먼저 진입
    //   ZADD 문법: ZADD <key> <score> <member>
    //   → queue:waiting에 member=userId, score=timestamp 로 저장
    //   → score 오름차순 정렬이므로 먼저 진입한 userId가 낮은 순번을 갖는다
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
