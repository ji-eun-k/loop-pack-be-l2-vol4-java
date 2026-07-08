# 대기열(Waiting Queue) 시스템 분석

---

## 1. 대기열이 왜 필요한가

선착순 구매처럼 트래픽이 특정 순간에 폭발적으로 몰리면, 모든 요청이 동시에 DB 커넥션을 잡으려 하다가 커넥션 풀이 고갈된다. 대기열은 이 문제를 "입장 순서를 정해서 N명씩 처리"로 바꿔준다.

```
[문제 상황]
1만 명 동시 요청 → DB 커넥션 10개 → 모두 대기 → timeout 폭발

[대기열 적용 후]
1만 명 → Redis 대기열에 순번 저장 → 30명씩 입장 승인 → DB 30개 요청만 처리
```

---

## 2. 핵심 개념: Redis Sorted Set

```
ZADD waiting-queue {입장요청시각(Redis TIME 기반 Unix timestamp µs)} {userId}

예시:
ZADD waiting-queue 1720000001000001 "1"
ZADD waiting-queue 1720000001000257 "2"
ZADD waiting-queue 1720000002000042 "3"
```

- `score` = 입장 요청 시각 → 먼저 온 사람이 작은 score → ZRANK로 순번 조회 (0-based)
- `value` = userId
- score는 애플리케이션 서버 시각(ms)이 아니라 **Lua 스크립트 내에서 Redis `TIME` 명령어로 얻은 마이크로초(µs) 단위 시각**을 사용한다 (이유는 3-1 참조)

| Redis 명령어 | 역할 |
|---|---|
| `ZADD` | 대기열 진입. 재진입 시 score 갱신 → 줄 맨 뒤로 밀림 |
| `ZRANK` | 내 순번 조회 (0-based, 없으면 null) |
| `ZCARD` | 전체 대기 인원 수 |
| `ZPOPMIN n` | 앞에서 n명 꺼내기 (스케줄러가 입장 토큰 발급할 때) |
| `SET key EX ttl` | 활성 토큰 저장 (입장 허가) |
| `GET key` | 활성 토큰 값(UUID) 조회 |
| `DEL key` | 주문 완료 후 토큰 삭제 |

---

## 3. Lua 스크립트로 원자적 연산 보장

Redis 명령어는 각각 원자적이지만, **여러 명령을 묶어야 하는 연산**은 중간에 다른 요청이 끼어들 수 있다. Lua 스크립트는 이 명령 묶음 전체를 Redis 내부에서 싱글 스레드로 실행시켜 원자성을 보장한다.

### 3-1. 대기열 진입 Lua (enter)

**왜 필요한가?**
ZADD + ZRANK + ZCARD를 따로 3번 호출하면 ZADD와 ZRANK 사이에 다른 사람이 진입해서 순번이 달라질 수 있다. 순번 반환까지 원자적으로 묶어야 한다.

```lua
-- KEYS[1] : "queue:waiting"
-- ARGV[1] : userId (string)

-- score는 Redis TIME 기반 마이크로초(µs) 시각
local t = redis.call('TIME')   -- {초, 마이크로초}
local score = tonumber(t[1]) * 1000000 + tonumber(t[2])

-- 재진입 시 score 갱신 → 줄 맨 뒤로 밀림 (의도된 동작)
redis.call('ZADD', KEYS[1], score, ARGV[1])

local rank  = redis.call('ZRANK', KEYS[1], ARGV[1])
local total = redis.call('ZCARD', KEYS[1])

return {rank, total}
```

**왜 score를 Java가 아닌 Redis `TIME`으로 계산하는가?**

처음에는 Java에서 `System.currentTimeMillis()`를 ARGV로 전달했으나, 동시 진입 테스트에서 순번 중복 버그가 발견되어 변경했다.

- **문제**: 여러 유저가 같은 millisecond에 진입하면 score가 완전히 동일해진다. Redis ZSet은 score가 같으면 member를 **문자열 사전순**으로 정렬하는데 (`"1" < "10" < "2" < "3"`), 이후 사전순으로 앞서는 userId가 진입하면 이미 순번을 안내받은 유저의 rank가 밀린다. 결과적으로 서로 다른 유저가 같은 순번을 안내받거나, 안내받은 순번이 뒤바뀐다.
- **해결**: Redis는 단일 스레드라서 Lua 스크립트 두 개가 동시에 실행될 수 없다. Lua 안에서 `TIME`(마이크로초 정밀도)으로 score를 계산하면 순차 실행이 보장되어 score 충돌이 사실상 불가능하다.
- **부수 효과**: 분산 환경에서 애플리케이션 서버가 여러 대여도 Redis 서버 시계 하나만 기준이 되므로, 서버 간 시계 오차(clock skew)로 인한 순서 왜곡도 함께 사라진다.

| 방식 | 정밀도 | 동시 진입 시 충돌 | 분산 환경 시계 오차 |
|---|---|---|---|
| Java `System.currentTimeMillis()` (ARGV 전달) | ms | 발생 (같은 ms → 같은 score) | 서버별 시계 차이 존재 |
| Redis `TIME` (Lua 내 계산) | µs + 순차 실행 보장 | 사실상 불가능 | Redis 시계 단일 기준 |


### 3-2. 스케줄러 입장 토큰 발급

ZPOPMIN은 그 자체로 원자적이라 한 번 꺼낸 userId는 다른 누가 가져갈 수 없다. SET은 userId마다 별개 연산이므로 Lua로 묶지 않는다.

```
1. ZPOPMIN queue:waiting N명
2. 각 userId마다:
   a. 0~300ms Jitter delay (동시에 몰리는 주문 API 호출 분산)
   b. UUID 생성
   c. SET queue:active:{userId} {uuid} EX {ttl}
```

```java
List<Long> userIds = queueRepository.popOldest(batchSize);
for (Long userId : userIds) {
    long jitterMillis = ThreadLocalRandom.current().nextLong(0, 301);
    Thread.sleep(jitterMillis);
    String token = UUID.randomUUID().toString();
    entryTokenRepository.save(userId, token, tokenTtlSeconds);
}
```


### 3-3. 순번 조회 Lua (position)

**왜 필요한가?**
ZRANK + ZCARD를 따로 2번 호출하면 두 호출 사이에 다른 사람이 입장해 카운트가 달라질 수 있다. 원자적으로 묶어야 한다.

토큰 조회는 이 Lua 스크립트에 포함하지 않는다. 스케줄러가 `ZPOPMIN`으로 유저를 꺼내는 순간 대기열에서 이미 사라지므로, 토큰 체크와 순번 조회는 서로 독립적인 연산이다. `QueueService`에서 두 Repository를 순서대로 호출해 상태를 결정한다.

```lua
-- KEYS[1] : "queue:waiting"
-- ARGV[1] : userId
-- returns: {-1} if not in queue, {rank(0-based), totalWaiting} if waiting

local rank = redis.call('ZRANK', KEYS[1], ARGV[1])
if rank == false then return {-1} end
local total = redis.call('ZCARD', KEYS[1])
return {rank, total}
```


---

## 4. 전체 플로우 다이어그램

### 4-1. 대기열 진입 (`POST /api/v1/queue/enter`)

```
클라이언트                 QueueController          Redis
    │                           │                     │
    │  POST /api/v1/queue/enter  │                     │
    │ ─────────────────────────> │                     │
    │                           │  [Lua: enter]        │
    │                           │  ZADD + ZRANK        │
    │                           │  + ZCARD (원자적)    │
    │                           │ ─────────────────── >│
    │                           │ <── {rank:42,total:100}
    │                           │                     │
    │  { status: WAITING,        │  (rank는 0-based,   │
    │    position: 43,           │   표시는 +1)         │
    │    waitingCount: 100,      │                     │
    │    estimatedWaitSeconds: 86 }  │                     │
    │ <───────────────────────── │                     │
```

### 4-2. 배치 스케줄러 입장 토큰 발급 (매 1초)

```
QueueScheduler                         Redis
      │                                  │
      │  ZPOPMIN queue:waiting N명        │
      │ ──────────────────────────────── >│
      │ <── [userId1, userId2, ..., userN]│
      │                                  │
      │  각 userId별 (순차, Jitter 포함): │
      │    sleep(0~300ms)                 │
      │    uuid = UUID.randomUUID()       │
      │    SET queue:active:{id} {uuid}   │
      │    EX 300                         │
      │ ──────────────────────────────── >│
      │                                  │
```

### 4-3. 순번 폴링 (`GET /api/v1/queue/position`)

`QueueService.getPosition()`은 두 Repository를 순서대로 조회한다:
① `EntryTokenRepository.find()` → 토큰 존재 시 ACTIVE
② `QueueRepository.findPositionSnapshot()` → 대기열 존재 시 WAITING
③ 둘 다 없으면 NOT_IN_QUEUE

```
클라이언트                QueueService          EntryTokenRepo    QueueRepo(Lua)
    │                          │                     │                  │
    │  GET /api/v1/queue/position                     │                  │
    │ ──────────────────────── >│                     │                  │
    │                           │  GET queue:active:{id}                │
    │                           │ ─────────────────── >                  │
    │                           │ <── null (아직 없음)│                  │
    │                           │                     │                  │
    │                           │  ZRANK + ZCARD (Lua)│                  │
    │                           │ ──────────────────────────────────── > │
    │                           │ <── {30, 100}       │                  │
    │                           │                     │                  │
    │  { status: WAITING,       │                     │                  │
    │    position: 31,          │                     │                  │
    │    waitingCount: 100,     │                     │                  │
    │    nextPollAfterMs: 2000, │                     │                  │
    │    estimatedWaitSeconds: 62 }                   │                  │
    │ <──────────────────────── │                     │                  │
    │                           │                     │                  │
    │  (2초 뒤 다시 폴링)        │                     │                  │
    │                           │                     │                  │
    │  GET /api/v1/queue/position                     │                  │
    │ ──────────────────────── >│                     │                  │
    │                           │  GET queue:active:{id}                │
    │                           │ ─────────────────── >                  │
    │                           │ <── "uuid-token"    │  ← 스케줄러가 발급│
    │                           │                     │                  │
    │  { status: ACTIVE,        │                     │                  │
    │    entryToken: "550e8400-e29b-41d4-a716-446655440000" }
    │ <──────────────────────── │                     │                  │
    │                           │                     │                  │
    │  (이제 주문 API 호출 가능)  │                     │                  │
```


### 4-4. 주문 토큰 검증 및 삭제

```mermaid
sequenceDiagram
    participant C as 클라이언트
    participant F as UserAuthFilter
    participant I as QueueTokenInterceptor
    participant O as OrderFacade
    participant R as Redis
    participant E as ApplicationEventPublisher
    participant L as QueueTokenCleanupListener

    C->>F: POST /api/v1/orders<br/>X-Loopers-LoginId/Pw<br/>X-Queue-Token: {uuid}
    F->>F: LoginId/Pw 검증
    alt 인증 실패
        F-->>C: 401 Unauthorized
    end

    F->>I: 인증 통과 (request에 User 저장)
    I->>R: GET queue:active:{userId}
    R-->>I: "stored-uuid"

    alt 토큰 불일치 or 없음
        I-->>C: 403 Forbidden
    end

    I->>O: 토큰 일치 → createOrder()
    O->>O: 주문 처리
    O->>E: publishEvent(OrderCompletedEvent(userId))
    E->>L: @EventListener 호출
    L->>R: DEL queue:active:{userId}
    O-->>C: 201 Created
```

> 토큰 삭제(`DEL`)가 실패하더라도 `tokenTtlSeconds(300s)` 후 자동 만료된다.


---

## 5. 컴포넌트 구조

```
apps/commerce-api/src/main/java/com/loopers/
├── domain/queue/
│   ├── QueueRepository.java        — enter / findPositionSnapshot 인터페이스
│   ├── EntryTokenRepository.java   — save / find / delete 인터페이스
│   ├── QueuePositionSnapshot.java  — rank(0-based), totalWaiting record
│   └── QueueStatus.java            — WAITING / ACTIVE / NOT_IN_QUEUE
│
├── infrastructure/queue/
│   ├── RedisQueueRepository.java       — Lua: enter(ZADD+ZRANK+ZCARD), findPositionSnapshot(ZRANK+ZCARD)
│   ├── RedisEntryTokenRepository.java  — opsForValue GET/SET EX/DEL (queue:active:{userId})
│   └── QueueLuaScripts.java            — Lua 스크립트 상수 정의
│
├── application/queue/
│   ├── QueueService.java         — enter / getPosition
│   ├── QueueScheduler.java       — @Scheduled, 1초마다 ZPOPMIN + 입장 토큰 발급
│   ├── QueueEntry.java           — status, position, waitingCount, estimatedWaitSeconds
│   ├── QueuePosition.java        — status, position, waitingCount, nextPollAfterMs, estimatedWaitSeconds, entryToken
│   └── QueueProperties.java      — @ConfigurationProperties(prefix = "queue")
│
├── interfaces/api/queue/
│   ├── QueueV1Controller.java    — POST /api/v1/queue/enter, GET /api/v1/queue/position
│   ├── QueueV1Dto.java
│   ├── QueueTokenInterceptor.java — /api/v1/orders/** 가드
│   └── QueueErrorType.java
│
└── application/order/
    ├── OrderCompletedEvent.java          — record(userId)
    └── QueueTokenCleanupListener.java    — @EventListener
```

---

## 6. 핵심 클래스 설계

### QueueService

```java
QueueEntry enter(Long userId);          // 대기열 진입, 재진입 시 줄 맨 뒤
QueuePosition getPosition(Long userId); // ① 토큰 조회 → ② 순번 조회 → ③ NOT_IN_QUEUE
```

```java
// getPosition 내부 로직
return entryTokenRepository.find(userId)                  // ① ACTIVE?
    .map(token -> new QueuePosition(ACTIVE, ..., token))
    .orElseGet(() -> queueRepository.findPositionSnapshot(userId)  // ② WAITING?
        .map(snapshot -> new QueuePosition(WAITING, ...))
        .orElse(new QueuePosition(NOT_IN_QUEUE, ...))        // ③
    );
```

### QueueEntry / QueuePosition

```java
record QueueEntry(
    QueueStatus status,       // WAITING or ACTIVE
    long position,            // 1-based 순번
    long waitingCount,
    long estimatedWaitSeconds
) {}

record QueuePosition(
    QueueStatus status,
    long position,
    long waitingCount,
    long nextPollAfterMs,     // 클라이언트 다음 폴링 간격 힌트
    long estimatedWaitSeconds,
    String entryToken         // ACTIVE일 때만 non-null, WAITING/NOT_IN_QUEUE는 null
) {}
```

### QueueProperties

```yaml
queue:
  enabled: true
  batch-size: 75           # 1초당 입장 허가 인원 (산정 근거 아래 참조)
  scheduler-interval-ms: 1000
  token-ttl-seconds: 300   # 활성 토큰 유효 시간 (5분)
  max-waiting-size: 100000
```

> **배치 크기 산정 근거**
>
> ---
>
> #### 1단계. 기준 지표 선택 — 왜 avg가 아닌 p95인가
>
> 대기열이 열리는 순간은 트래픽이 가장 폭발적으로 몰리는 시점이다.
> 이 때는 DB 경합, 락 대기, 커넥션 풀 경쟁이 모두 겹쳐 처리 시간이 평소보다 길어진다.
>
> - **avg(231ms)** 는 부하가 낮을 때의 빠른 요청들이 포함되어 있어 실제보다 낙관적이다.
> - **p95(371ms)** 는 "100개 요청 중 95개가 이 시간 안에 끝난다"는 의미로,
>   트래픽 스파이크 상황에서도 커넥션 풀이 감당할 수 있는 처리량의 현실적 하한이다.
>
> batchSize를 avg 기준으로 높게 잡으면, 트래픽이 몰릴 때 실제 처리 속도보다 더 많은 인원을
> 입장시켜 커넥션 풀이 포화되는 역효과가 생긴다.
>
> ---
>
> #### 2단계. k6 실측 결과 (VUs=40, duration=30s)
>
> | 지표 | 값 |
> |---|---|
> | avg | 231ms |
> | p50 | 221ms |
> | p90 | 332ms |
> | **p95** | **371ms** ← 기준 채택 |
> | max | 645ms |
> | 처리량 | 173 orders/s |
> | 에러율 | 0% |
>
> `OrderFacade.createOrder()` 트랜잭션 내 DB 연산 (1상품 기준):
> 1. SELECT product
> 2. SELECT FOR UPDATE product_stock
> 3. UPDATE product_stock
> 4. INSERT order
> 5. INSERT order_item
>
> ---
>
> #### 3단계. 공식 및 계산
>
> ```
> 실효 TPS   = 커넥션 풀 수 / (기준 처리 시간(ms) / 1000)
> 대기열 TPS = 실효 TPS × 커넥션 할당 비율
> batchSize  = 대기열 TPS × (스케줄러 주기(ms) / 1000)
> ```
>
> **커넥션 할당 비율(70%)이 필요한 이유**
> 풀 40개를 주문에 100% 쓰면 상품 조회·유저 인증 등 다른 API에 커넥션이 없어 전체가 터짐.
> 나머지 30% (12개)는 조회성 API용으로 예약.
>
> **p95 기준 계산**
> ```
> 실효 TPS   = 40 / (371 / 1000)
>            = 40 / 0.371
>            ≈ 107.8 TPS
>
> 대기열 TPS = 107.8 × 0.7
>            ≈ 75.5 TPS
>
> batchSize  = 75.5 × (1000 / 1000)
>            = 75.5
>            → 75명/틱  (소수점 내림, 보수적 선택)
> ```
>
> ---
>
> #### 4단계. avg vs p95 비교
>
> | 기준 | 처리 시간 | 실효 TPS | × 70% | batchSize |
> |---|---|---|---|---|
> | avg | 231ms | 173 | 121 | **121명/틱** |
> | p90 | 332ms | 120 | 84 | **84명/틱** |
> | **p95** | **371ms** | **108** | **75** | **75명/틱** ← 채택 |
>
> p95 기준 75명/틱은 avg 기준보다 38% 보수적이다.
> 트래픽 스파이크 구간에서도 커넥션 풀을 70% 이내로 유지할 수 있다.
>
> ---
>
> #### 5단계. 트레이드오프
>
> batchSize를 낮추면 커넥션 풀 안정성이 올라가지만 유저 대기 시간이 늘어난다.
>
> | 항목 | avg 기준(121) | p95 기준(75) |
> |---|---|---|
> | 1000번째 유저 예상 대기 | ≈ 9초 | ≈ 14초 |
> | 커넥션 풀 여유 | 낮음 | 높음 |
> | 스파이크 시 오버로드 위험 | 있음 | 낮음 |


### QueueLuaScripts

```java
public final class QueueLuaScripts {

    // KEYS[1]: "queue:waiting", ARGV[1]: userId
    // score: Redis TIME 기반 µs 시각 (Lua 내 계산 — 이유는 3-1 참조)
    // returns: {rank(0-based), totalCount}
    public static final String ENTER = """
        local t = redis.call('TIME')
        local score = tonumber(t[1]) * 1000000 + tonumber(t[2])
        redis.call('ZADD', KEYS[1], score, ARGV[1])
        local rank  = redis.call('ZRANK', KEYS[1], ARGV[1])
        local total = redis.call('ZCARD', KEYS[1])
        return {rank, total}
        """;

    // KEYS[1]: "queue:waiting", ARGV[1]: userId
    // returns: {-1} if not in queue, {rank(0-based), totalWaiting} if waiting
    public static final String POSITION = """
        local rank = redis.call('ZRANK', KEYS[1], ARGV[1])
        if rank == false then return {-1} end
        local total = redis.call('ZCARD', KEYS[1])
        return {rank, total}
        """;

    private QueueLuaScripts() {}
}
```

### Polling 주기 동적 조절

순번이 멀수록 폴링 간격을 늘려 서버 부하를 줄인다.

```
position 500+  → nextPollAfterMs = 10,000 (10초)
position 100+  → nextPollAfterMs = 5,000  (5초)
position 30+   → nextPollAfterMs = 2,000  (2초)
position 1~29  → nextPollAfterMs = 1,000  (1초)
```


---

## 7. Redis Key 설계

| Key | 형태 | 설명 |
|---|---|---|
| `queue:waiting` | Sorted Set | score = 입장 시각 (Redis TIME 기반 µs), member = userId |
| `queue:active:{userId}` | String | 값 = UUID 토큰, TTL = 300초 |

---

## 8. 예상 대기 시간 계산

```
예상 대기 시간(초) = position / batchSize × (schedulerIntervalMs / 1000)

예시:
- position: 363 (1-based)
- batchSize: 75명/틱
- schedulerIntervalMs: 1000ms

→ 363 / 75 × 1 = 4.84 ≈ 5초
```


---

## 9. SSE 기반 실시간 순번 PUSH

### 9-1. Polling vs SSE 비교

| 항목 | Polling (`GET /position`) | SSE (`GET /position/stream`) |
|---|---|---|
| 연결 방식 | 클라이언트가 주기적으로 요청 | 서버가 스케줄러 틱마다 PUSH |
| 불필요한 요청 | 있음 (변화 없어도 매 N초 요청) | 없음 |
| 서버 리소스 | 요청당 연결/해제 | HTTP 커넥션 유지 + 연결 수만큼 Redis 조회 |
| 구현 복잡도 | 낮음 | 중간 |
| 적용 환경 | 멀티 인스턴스 포함 모든 환경 | 단일 인스턴스 (멀티는 Redis Pub/Sub 필요) |

### 9-2. 전체 플로우

```
클라이언트                  QueueV1Controller     SseEmitterRegistry    QueueScheduler
    │                              │                     │                   │
    │  GET /position/stream         │                     │                   │
    │ ─────────────────────────── > │                     │                   │
    │                              │ getPosition()        │                   │
    │                              │ ① 초기 "position" 전송                   │
    │  event: position              │                     │                   │
    │  data: {WAITING, pos=43, ...} │                     │                   │
    │ < ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ │                     │                   │
    │                              │ ② register(userId)   │                   │
    │                              │ ──────────────────── >                   │
    │  (연결 유지 중)                │                     │                   │
    │                              │                      │  (매 1초 스케줄러) │
    │                              │                      │  ZPOPMIN N명      │
    │                              │                      │  토큰 발급        │
    │                              │                      │  sendActive()     │
    │                              │                      │                   │
    │                              │  ③ 남은 WAITING 유저들에게 sendPosition() │
    │                              │                      │  getPosition()    │
    │  event: position              │                      │  per userId       │
    │  data: {WAITING, pos=42, ...} │                      │                   │
    │ < ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ │                      │                   │
    │  (nextPushAtMs에 따라 반복)    │                      │                   │
    │                              │                      │                   │
    │  event: active               │                      │  (내 차례 도달)    │
    │  data: {ACTIVE, token=uuid}   │                      │  sendActive()     │
    │ < ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ │                      │                   │
    │  (연결 종료)                   │                      │                   │
    │                              │                      │                   │
    │  → POST /api/v1/orders        │                      │                   │
    │    X-Queue-Token: uuid        │                      │                   │
```

### 9-3. 상태별 SSE 응답 동작

| 연결 시 상태 | 초기 이벤트 | 이후 동작 |
|---|---|---|
| `WAITING` | `position` 이벤트 (순번 정보) | 연결 유지 → 스케줄러 틱마다 `position` PUSH, 입장 가능 시 `active` PUSH 후 종료 |
| `ACTIVE` | `position` 이벤트 (ACTIVE + token) | 즉시 연결 종료 |
| `NOT_IN_QUEUE` | `position` 이벤트 (NOT_IN_QUEUE) | 즉시 연결 종료 |

### 9-4. PUSH 주기 (순번 구간별 적응형)

폴링의 `nextPollAfterMs`와 동일한 기준을 SSE PUSH 간격에 그대로 재사용한다.
`SseEmitterRegistry`가 userId별 `nextPushAtMs`(다음 PUSH 허용 시각)을 관리하며,
스케줄러가 매 틱 `sendPosition()`을 호출해도 시각이 되지 않은 유저는 스킵한다.

| position | nextPollAfterMs | PUSH 빈도 |
|---|---|---|
| ≤ 50 | 1,000ms | 매 틱 (1초마다) |
| ≤ 500 | 3,000ms | 3틱에 1번 |
| ≤ 5,000 | 5,000ms | 5틱에 1번 |
| 초과 | 10,000ms | 10틱에 1번 |

### 9-5. 이벤트 포맷

**"position" 이벤트** (연결 즉시 + 스케줄러 틱마다)
```
event: position
data: {"status":"WAITING","position":43,"waitingCount":150,"nextPollAfterMs":3000,"estimatedWaitSeconds":2,"entryToken":null}
```

**"active" 이벤트** (스케줄러가 토큰 발급 후 PUSH, 이후 연결 종료)
```
event: active
data: {"status":"ACTIVE","entryToken":"550e8400-e29b-41d4-a716-446655440000"}
```

### 9-6. 컴포넌트 설계

```java
// SseEmitterRegistry (application/queue)
ConcurrentHashMap<Long, SseEmitter>  emitters      // userId → HTTP 커넥션 핸들
ConcurrentHashMap<Long, AtomicLong>  nextPushAtMs  // userId → 다음 PUSH 허용 시각(ms)

register(userId, emitter)         // 컨트롤러가 WAITING 시 등록, 종료 콜백으로 자동 cleanup
getRegisteredUserIds()            // 스케줄러가 순회할 대상 스냅샷 반환
sendPosition(userId, position)    // nextPushAtMs 체크 후 "position" 이벤트 전송
sendActive(userId, token)         // "active" 이벤트 전송 + complete (연결 종료)
```

```java
// QueueScheduler — 매 틱 실행 순서
1. popOldest(batchSize)           // 대기열 앞에서 N명 꺼냄
2. 각 userId: jitter → 토큰 발급 → sendActive()   // ACTIVE 유저 처리, registry에서 제거됨
3. getRegisteredUserIds() 순회    // 남은 WAITING SSE 연결자
   → getPosition(userId)          // Redis에서 현재 순번 조회
   → sendPosition(userId, pos)    // nextPushAtMs 기준으로 전송 or 스킵
```

### 9-7. 타임아웃과 재연결

SSE 연결은 60초 후 타임아웃된다. 75명/초 처리 기준으로 position 4,500 이내인 유저는 60초 내에 ACTIVE를 받는다. 그 이상 대기가 필요한 유저는 재연결해야 하며, 재연결 시 초기 `position` 이벤트로 현재 순번을 다시 받는다.

### 9-8. 단일 인스턴스 한계

`SseEmitterRegistry`가 인메모리(`ConcurrentHashMap`)이므로 멀티 인스턴스 환경에서는 emitter가 등록된 인스턴스와 스케줄러가 실행되는 인스턴스가 다르면 PUSH가 도달하지 않는다.

**해결 방향**:
1. 스케줄러 단일 실행 보장 (ShedLock 등으로 한 인스턴스만 스케줄러 실행)
2. 스케줄러가 토큰 발급/순번 갱신 후 Redis Pub/Sub 채널에 발행
3. 모든 인스턴스가 채널을 구독하다가 자신의 registry에 해당 userId가 있으면 PUSH
