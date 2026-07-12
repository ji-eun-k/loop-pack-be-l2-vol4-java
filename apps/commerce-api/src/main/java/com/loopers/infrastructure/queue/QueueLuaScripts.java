package com.loopers.infrastructure.queue;

public final class QueueLuaScripts {

    // KEYS[1]: "queue:waiting" — Sorted Set 키
    // ARGV[1]: userId (string) — Sorted Set의 member (값). key가 아님에 주의
    // score: Redis 서버 시각(TIME) 기반 Unix timestamp µs — 정렬 기준. 작을수록 먼저 진입
    //   Java 서버 시각(ms) 대신 Redis TIME(µs)을 쓰는 이유:
    //   - Redis는 단일 스레드라 Lua 스크립트가 동시에 실행되지 않음 → 동일 score 충돌 없음
    //     (ms 단위로 동시 진입 시 score가 겹치면 사전순 정렬로 순번이 중복/역전됨)
    //   - 분산 환경에서도 Redis 시계 하나만 기준이 되어 서버 간 시계 차이 문제 없음
    //   ZADD 문법: ZADD <key> <score> <member>
    //   → score 오름차순 정렬이므로 먼저 진입한 userId가 낮은 순번을 갖는다
    // returns: {rank(0-based), totalCount}
    public static final String ENTER = """
            local t = redis.call('TIME')
            local score = tonumber(t[1]) * 1000000 + tonumber(t[2])
            redis.call('ZADD', KEYS[1], score, ARGV[1])
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
