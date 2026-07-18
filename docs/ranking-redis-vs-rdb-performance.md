# 상품 랭킹 Redis vs RDB 성능 비교

## 1. 결론

10만 개 상품의 일간 랭킹을 조회한 로컬 측정에서 Redis가 모든 읽기 연산에서 빨랐다.

- 첫 페이지는 Redis가 RDB보다 평균 **1.38배** 빨랐다.
- 8만 번째 상품부터 조회하는 깊은 페이지는 **14.90배** 빨랐다.
- 단일 상품 순위 조회는 **78.85배** 빨랐다.
- 전체 랭킹 수 조회는 **19.24배** 빨랐다.
- 8개 스레드가 같은 상품 점수를 갱신할 때 Redis 처리량은 RDB의 **113.01배**였다.
- 갱신 대상 상품이 모두 달라도 Redis 처리량은 RDB의 **37.53배**였다.

첫 페이지 조회만 필요하고 데이터 규모가 작다면 RDB도 충분히 실용적이다. 그러나 현재 기능처럼 깊은 페이지와 상품 상세의 실시간 순위 조회를 함께 제공한다면 Redis ZSET이 더 적합하다. RDB는 장기 보관, 분석, 복구용 원본으로 사용하는 구성이 합리적이다.

## 2. RDB 구현

`daily_product_ranking` 테이블은 날짜와 상품 ID를 복합 기본 키로, 점수를 값으로 가진다.

```sql
CREATE TABLE daily_product_ranking (
    ranking_date DATE NOT NULL,
    product_id BIGINT NOT NULL,
    score DOUBLE NOT NULL,
    PRIMARY KEY (ranking_date, product_id),
    INDEX idx_daily_ranking_date_score_product
        (ranking_date, score DESC, product_id DESC)
);

CREATE TABLE daily_ranking_handled_event (
    ranking_date DATE NOT NULL,
    event_id VARCHAR(100) NOT NULL,
    PRIMARY KEY (ranking_date, event_id),
    INDEX idx_ranking_handled_event_date (ranking_date)
);
```

동점이면 `product_id DESC`를 보조 정렬 기준으로 사용해 결과를 결정적으로 만든다. 테스트와 로컬 프로필은 Hibernate가 엔티티로 테이블을 생성한다. `ddl-auto=none`인 배포 환경에서는 위 DDL을 별도 마이그레이션으로 적용해야 한다.

조회 저장소는 설정으로 선택한다.

```yaml
ranking:
  repository: redis # rdb로 변경하면 RDB 조회 구현 사용
```

RDB 구현의 핵심 쿼리는 다음과 같다.

| 기능 | RDB | Redis |
|---|---|---|
| 페이지 | 날짜 인덱스 범위를 점수순으로 `LIMIT/OFFSET` | `ZREVRANGE WITHSCORES` |
| 상품 순위 | 날짜 전체에 `ROW_NUMBER()`를 계산 후 상품 필터 | `ZREVRANK` |
| 전체 수 | 날짜 범위 `COUNT(*)` | `ZCARD` |

현재 이벤트 소비기는 Redis에 점수를 반영한다. 따라서 `ranking.repository=rdb`를 실제 서비스에 적용하려면 이벤트 소비 결과를 위 테이블에도 적재해야 한다. 이번 구현과 비교는 **동일 데이터가 이미 적재된 읽기 모델의 조회 성능**을 분리해 측정한 것이다.

## 3. 측정 방법

- 실행일: 2026-07-15
- 런타임: Amazon Corretto 21.0.11
- 저장소: Testcontainers MySQL 8.0, Redis `latest`
- 데이터: 한 날짜에 상품 100,000개, 상품별 서로 다른 점수
- 워밍업: 연산별 각 저장소 20회
- 본 측정: 연산별 각 저장소 100회, 단일 스레드
- 쓰기 측정: 8개 스레드 × 100개 이벤트, 총 800개 이벤트
- 측정 범위: 저장소 메서드 호출부터 결과 역직렬화까지
- 페이지 크기: 20개
- 깊은 페이지: page=4,000, 즉 OFFSET 80,000

실행 명령:

```powershell
.\gradlew.bat :apps:commerce-api:test --tests "com.loopers.infrastructure.ranking.RankingRepositoryPerformanceBenchmark"
```

## 4. 측정 결과

단위는 마이크로초(µs)이며 낮을수록 좋다.

| 연산 | RDB 평균 | RDB p50 | RDB p95 | Redis 평균 | Redis p50 | Redis p95 | 평균 속도 차이 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 첫 페이지 | 1,124.84 | 907.10 | 1,987.50 | 814.23 | 673.80 | 1,441.40 | Redis 1.38배 |
| 깊은 페이지 | 11,152.38 | 10,699.30 | 13,968.40 | 748.48 | 620.10 | 1,031.80 | Redis 14.90배 |
| 단일 상품 순위 | 71,165.64 | 70,654.60 | 74,477.60 | 902.50 | 525.00 | 3,084.10 | Redis 78.85배 |
| 전체 수 | 10,920.32 | 10,315.60 | 13,098.80 | 567.47 | 454.50 | 996.30 | Redis 19.24배 |

### 4.1 이벤트 점수 갱신 결과

각 이벤트는 고유 `eventId` 하나와 상품 점수 delta 하나를 가진다. RDB는 이벤트마다 하나의 트랜잭션 안에서 아래 작업을 수행했다.

1. `daily_ranking_handled_event`에 `INSERT IGNORE`하여 멱등성 확인
2. 새 이벤트이면 `INSERT ... ON DUPLICATE KEY UPDATE score = score + 1.0`
3. 커밋

Redis는 현재 운영 구현과 같은 의미의 Lua에서 `SADD`, `ZINCRBY`, `EXPIRE`를 원자적으로 실행했다.

| 부하 형태 | RDB 경과시간 | RDB 처리량 | Redis 경과시간 | Redis 처리량 | 처리량 차이 |
|---|---:|---:|---:|---:|---:|
| 같은 상품 집중 | 12,539.15 ms | 63.80 ops/s | 110.96 ms | 7,209.88 ops/s | Redis 113.01배 |
| 서로 다른 상품 분산 | 2,932.52 ms | 272.80 ops/s | 78.14 ms | 10,237.76 ops/s | Redis 37.53배 |

같은 상품 집중 시 RDB의 단일 랭킹 행에 배타적 잠금 요청이 몰려 트랜잭션이 직렬화된다. 대상 상품이 분산되면 행 잠금 경합이 줄어 처리량이 약 4.3배 개선되지만, 멱등성 INSERT와 UPSERT, 트랜잭션 커밋 비용은 계속 발생한다.

이 쓰기 측정은 Kafka 전송·소비 시간을 제외한 저장소 반영 구간만 비교한다. 또한 이벤트마다 한 번 호출한 결과다. 현재 Redis `RankingUpdater.applyEvents()`는 같은 날짜의 여러 Kafka 이벤트를 Lua 한 번으로 묶으므로, 실제 배치 소비에서는 Redis 왕복 비용이 이 측정보다 더 낮아질 수 있다. RDB도 여러 이벤트를 한 트랜잭션으로 묶을 수 있지만 트랜잭션 실패 범위가 커지고, 같은 상품 행의 잠금 경합 자체는 사라지지 않는다.

## 5. 차이가 발생하는 이유

Redis ZSET은 점수순 정렬 상태를 유지한다. 페이지 조회는 정렬된 자료구조의 범위를 읽고, 개별 순위는 멤버의 순위를 직접 찾으며, 개수는 자료구조 메타데이터를 읽는다.

RDB의 첫 페이지는 복합 인덱스 앞부분만 읽으므로 차이가 작다. 반면 OFFSET 80,000은 결과를 반환하기 전에 앞의 80,000개 인덱스 항목을 건너뛰어야 한다. 단일 순위도 해당 날짜의 행에 순번을 계산해야 하므로 데이터 수에 비례하는 비용이 발생한다. `COUNT(*)` 역시 날짜 범위의 인덱스 항목을 세어야 한다.

## 6. 해석 시 주의사항

이 결과는 로컬 Docker 환경의 마이크로 벤치마크이며 JMH나 운영 부하 테스트 결과가 아니다. 컨테이너 자원, 네트워크, Redis 이미지 버전, MySQL 버퍼 풀 상태에 따라 절대 수치는 달라진다. 또한 동시 요청, 점수 갱신 성능, 저장 내구성, 장애 복구 비용은 이번 측정에 포함하지 않았다.

운영 결정을 위해서는 API 수준에서 예상 트래픽과 읽기/쓰기 비율을 반영한 k6 테스트를 추가하고, Redis와 MySQL의 CPU·메모리·커넥션·InnoDB row lock wait 지표를 함께 비교해야 한다.

## 7. 관련 코드

- Redis 조회: `apps/commerce-api/src/main/java/com/loopers/infrastructure/ranking/RedisRankingRepository.java`
- RDB 조회: `apps/commerce-api/src/main/java/com/loopers/infrastructure/ranking/RdbRankingRepository.java`
- RDB 엔티티와 인덱스: `apps/commerce-api/src/main/java/com/loopers/infrastructure/ranking/RdbRankingEntity.java`
- RDB 이벤트 멱등성 엔티티: `apps/commerce-api/src/main/java/com/loopers/infrastructure/ranking/RdbRankingHandledEventEntity.java`
- 동작 검증: `apps/commerce-api/src/test/java/com/loopers/infrastructure/ranking/RdbRankingRepositoryIntegrationTest.java`
- 성능 비교: `apps/commerce-api/src/test/java/com/loopers/infrastructure/ranking/RankingRepositoryPerformanceBenchmark.java`
