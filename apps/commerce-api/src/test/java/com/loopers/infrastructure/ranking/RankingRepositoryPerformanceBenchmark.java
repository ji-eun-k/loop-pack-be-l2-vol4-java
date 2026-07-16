package com.loopers.infrastructure.ranking;

import com.loopers.support.ranking.RankingKeys;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import com.loopers.utils.RedisCleanUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

@SpringBootTest
@Import(RedisTestContainersConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RankingRepositoryPerformanceBenchmark {

    private static final LocalDate DATE = LocalDate.of(2026, 7, 15);
    private static final int DATA_SIZE = 100_000;
    private static final int WARMUP = 20;
    private static final int ITERATIONS = 100;
    private static final int CONCURRENCY = 8;
    private static final int EVENTS_PER_THREAD = 100;
    private static final DefaultRedisScript<Long> APPLY_ONE_EVENT_SCRIPT = new DefaultRedisScript<>("""
        if redis.call('SADD', KEYS[2], ARGV[1]) == 1 then
            redis.call('ZINCRBY', KEYS[1], ARGV[3], ARGV[2])
            redis.call('EXPIRE', KEYS[1], ARGV[4])
            redis.call('EXPIRE', KEYS[2], ARGV[4])
            return 1
        end
        return 0
        """, Long.class);

    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private RedisTemplate<String, String> redisTemplate;
    @Autowired private DatabaseCleanUp databaseCleanUp;
    @Autowired private RedisCleanUp redisCleanUp;
    @Autowired private PlatformTransactionManager transactionManager;
    @Autowired private RankingTopSnapshotRepository snapshotRepository;

    private RdbRankingRepository rdb;
    private RedisRankingRepository redis;

    @BeforeAll
    void setUp() {
        rdb = new RdbRankingRepository(jdbcTemplate);
        redis = new RedisRankingRepository(redisTemplate, snapshotRepository);
        seedRdb();
        seedRedis();
    }

    @AfterAll
    void tearDown() {
        databaseCleanUp.truncateAllTables();
        redisCleanUp.truncateAll();
    }

    @Test
    void compareReadOperations() {
        benchmark("first page", () -> rdb.findPage(DATE, 0, 20), () -> redis.findPage(DATE, 0, 20));
        benchmark("deep page", () -> rdb.findPage(DATE, 4_000, 20), () -> redis.findPage(DATE, 4_000, 20));
        benchmark("single rank", () -> rdb.findRank(DATE, 50_000L), () -> redis.findRank(DATE, 50_000L));
        benchmark("total count", () -> rdb.countTotal(DATE), () -> redis.countTotal(DATE));
    }

    @Test
    void compareConcurrentEventUpdates() throws Exception {
        compareConcurrentWrite("hot product", false, DATE.plusDays(1));
        compareConcurrentWrite("distributed", true, DATE.plusDays(2));
    }

    private void compareConcurrentWrite(String label, boolean distributed, LocalDate date) throws Exception {
        long rdbNanos = runConcurrent("rdb-" + label, (thread, sequence) ->
            applyRdbEvent(date, "rdb-" + label + "-" + thread + "-" + sequence,
                distributed ? thread * EVENTS_PER_THREAD + sequence + 1L : 1L));
        long redisNanos = runConcurrent("redis-" + label, (thread, sequence) ->
            applyRedisEvent(date, "redis-" + label + "-" + thread + "-" + sequence,
                distributed ? thread * EVENTS_PER_THREAD + sequence + 1L : 1L));
        int totalEvents = CONCURRENCY * EVENTS_PER_THREAD;
        System.out.printf(
            "WRITE BENCHMARK %-11s | RDB elapsed=%8.2f ms throughput=%8.2f ops/s | Redis elapsed=%8.2f ms throughput=%8.2f ops/s%n",
            label, rdbNanos / 1_000_000.0, throughput(totalEvents, rdbNanos),
            redisNanos / 1_000_000.0, throughput(totalEvents, redisNanos)
        );
    }

    private long runConcurrent(String threadPrefix, EventAction action) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY,
            Thread.ofPlatform().name(threadPrefix + "-", 0).factory());
        CountDownLatch ready = new CountDownLatch(CONCURRENCY);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(CONCURRENCY);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            for (int thread = 0; thread < CONCURRENCY; thread++) {
                int threadNumber = thread;
                executor.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        for (int sequence = 0; sequence < EVENTS_PER_THREAD; sequence++) {
                            action.apply(threadNumber, sequence);
                        }
                    } catch (Exception e) {
                        failure.compareAndSet(null, e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            long startedAt = System.nanoTime();
            start.countDown();
            done.await();
            long elapsed = System.nanoTime() - startedAt;
            if (failure.get() != null) {
                throw new IllegalStateException("Concurrent benchmark failed", failure.get());
            }
            return elapsed;
        } finally {
            executor.shutdownNow();
        }
    }

    private void applyRdbEvent(LocalDate date, String eventId, long productId) {
        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            int inserted = jdbcTemplate.update(
                "INSERT IGNORE INTO daily_ranking_handled_event (ranking_date, event_id) VALUES (?, ?)",
                date, eventId
            );
            if (inserted == 1) {
                jdbcTemplate.update(
                    """
                        INSERT INTO daily_product_ranking (ranking_date, product_id, score) VALUES (?, ?, 1.0)
                        ON DUPLICATE KEY UPDATE score = score + 1.0
                        """,
                    date, productId
                );
            }
        });
    }

    private void applyRedisEvent(LocalDate date, String eventId, long productId) {
        redisTemplate.execute(
            APPLY_ONE_EVENT_SCRIPT,
            List.of(RankingKeys.daily(date), RankingKeys.handled(date)),
            eventId, Long.toString(productId), "1.0", "172800"
        );
    }

    private double throughput(int operations, long nanos) {
        return operations / (nanos / 1_000_000_000.0);
    }

    @FunctionalInterface
    private interface EventAction {
        void apply(int thread, int sequence) throws Exception;
    }

    private void benchmark(String operation, Runnable rdbAction, Runnable redisAction) {
        for (int i = 0; i < WARMUP; i++) {
            rdbAction.run();
            redisAction.run();
        }
        long[] rdbTimes = measure(rdbAction);
        long[] redisTimes = measure(redisAction);
        System.out.printf(
            "BENCHMARK %-12s | RDB avg=%8.2f us p50=%8.2f us p95=%8.2f us | Redis avg=%8.2f us p50=%8.2f us p95=%8.2f us%n",
            operation, avg(rdbTimes), percentile(rdbTimes, 50), percentile(rdbTimes, 95),
            avg(redisTimes), percentile(redisTimes, 50), percentile(redisTimes, 95)
        );
    }

    private long[] measure(Runnable action) {
        long[] times = new long[ITERATIONS];
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            action.run();
            times[i] = System.nanoTime() - start;
        }
        Arrays.sort(times);
        return times;
    }

    private double avg(long[] values) {
        return Arrays.stream(values).average().orElse(0) / 1_000.0;
    }

    private double percentile(long[] values, int percentile) {
        return values[(int) Math.ceil(values.length * percentile / 100.0) - 1] / 1_000.0;
    }

    private void seedRdb() {
        jdbcTemplate.batchUpdate(
            "INSERT INTO daily_product_ranking (ranking_date, product_id, score) VALUES (?, ?, ?)",
            new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                public void setValues(java.sql.PreparedStatement ps, int i) throws java.sql.SQLException {
                    long productId = i + 1L;
                    ps.setObject(1, DATE);
                    ps.setLong(2, productId);
                    ps.setDouble(3, DATA_SIZE - productId);
                }
                public int getBatchSize() { return DATA_SIZE; }
            }
        );
    }

    private void seedRedis() {
        byte[] key = RankingKeys.daily(DATE).getBytes(StandardCharsets.UTF_8);
        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (long productId = 1; productId <= DATA_SIZE; productId++) {
                connection.zSetCommands().zAdd(
                    key, DATA_SIZE - productId, Long.toString(productId).getBytes(StandardCharsets.UTF_8)
                );
            }
            return null;
        });
    }
}
