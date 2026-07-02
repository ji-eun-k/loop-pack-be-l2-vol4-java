# Transactional Outbox + Kafka 설계 정리

## 1. 왜 이 구조를 만들었는가

### 문제 상황
주문 완료, 좋아요, 상품 조회 같은 비즈니스 이벤트가 발생하면 두 가지 일이 동시에 이뤄져야 한다.

1. DB 트랜잭션 커밋 (주문 저장, 재고 차감 등)
2. Kafka 발행 (카탈로그 메트릭 반영, 쿠폰 발급 요청 등)

이 두 작업은 서로 다른 시스템이므로 둘 다 "한 트랜잭션"으로 묶을 수 없다. 그러면 다음 상황이 생긴다.

- DB 커밋 성공 → Kafka 발행 실패 → 메트릭 누락, 쿠폰 미발급
- Kafka 발행 성공 → DB 커밋 실패 → 없는 주문에 대한 이벤트 발행 (유령 이벤트)

### 해결 방향: Transactional Outbox Pattern
DB와 Kafka를 "같은 트랜잭션"처럼 다루는 게 목표다. 방법은 다음과 같다.

> 이벤트를 Kafka에 직접 보내지 않고, **같은 DB 트랜잭션 안에서 outbox 테이블에 저장**한다.
> 이후 **별도 스케줄러**가 outbox를 폴링하여 Kafka로 발행하고, 성공하면 PUBLISHED로 상태를 변경한다.

이렇게 하면 DB 커밋이 실패하면 outbox 레코드도 함께 롤백되므로 유령 이벤트가 없다.
Kafka 발행이 실패해도 outbox는 PENDING 상태로 남아 재시도된다.

---

## 2. 전체 서비스 구조

```
┌──────────────────────────────────────────────────────────────┐
│                        commerce-api                          │
│                                                              │
│  [1] 비즈니스 트랜잭션                                         │
│  OrderService / LikeFacade / ProductFacade                   │
│       └─ applicationEventPublisher.publishEvent(...)         │
│                                                              │
│  [2] 트랜잭션 커밋 후 Outbox 저장                              │
│  UserActionEventListener (@TransactionalEventListener)       │
│       └─ outboxService.save(outboxEvent)  ── [DB outbox 테이블] │
│                                                              │
│  [3] PRODUCT_VIEWED는 Outbox 없이 Kafka 직접 발행             │
│  UserActionEventListener (@EventListener)                    │
│       └─ kafkaTemplate.send("catalog-view-events-v1", ...)   │
│                                                              │
│  [4] Outbox 폴링 → Kafka 발행                                 │
│  OutboxEventPublishScheduler (@Scheduled fixedDelay=1s)      │
│       └─ findPending() → kafkaTemplate.send(...).get()       │
│                                                              │
│  [5] 쿠폰 발급 요청은 Outbox 없이 Kafka 직접 발행              │
│  CouponIssueFacade                                           │
│       └─ kafkaTemplate.send("coupon-issue-requests", ...)    │
└──────────────────────────────────────────────────────────────┘
            │                          │
            ▼                          ▼
   catalog-events-v1         catalog-view-events-v1
   coupon-issue-requests
            │
            ▼
┌──────────────────────────────────────────────────────────────┐
│                      commerce-streamer                       │
│                                                              │
│  CatalogMetricsConsumer  ← catalog-events-v1                 │
│       └─ CatalogMetricsProcessor (멱등 처리, EventHandled)    │
│                                                              │
│  CatalogViewConsumer  ← catalog-view-events-v1               │
│       └─ productMetricsJpaRepository.upsertViewCountIncrement│
│                                                              │
│  CouponIssueConsumer  ← coupon-issue-requests                │
│       └─ CouponIssueProcessor (Redis 재고 차감 + DB 저장)     │
│                                                              │
│  DlqPublisher  ─ 처리 실패 시 {topic}.dlq 토픽으로 격리       │
└──────────────────────────────────────────────────────────────┘
```

---

## 3. 이벤트 흐름별 상세 설명

### 3-1. 주문 완료 / 좋아요 (Outbox 경로)

```
OrderService.confirm()
  │  └─ publishEvent(UserActionEvent(ORDER_COMPLETED, userId, orderId))
  │
  │  트랜잭션 커밋
  │
UserActionEventListener.handle()   @TransactionalEventListener(AFTER_COMMIT) + @Async
  │  └─ OutboxEvent 생성 → outboxService.save()
  │       - eventType: "OrderItemSoldEvent" / "ProductLikedEvent" / "ProductUnlikedEvent"
  │       - topicName: "catalog-events-v1"
  │       - partitionKey: orderId.toString() / userId.toString()
  │       - status: PENDING
  │
OutboxEventPublishScheduler.publish()   @Scheduled(fixedDelay=1s)
  │  └─ findPending() → SELECT ... WHERE status='PENDING' OR (status='FAILED' AND retryCount < 5)
  │                       ORDER BY id ASC LIMIT 500
  │                       LOCK IN SKIP LOCKED  (다중 인스턴스 중복 처리 방지)
  │
  └─ kafkaTemplate.send(record).get()   (동기 발행, 실패 시 예외 던짐)
       headers: X-Event-Type, X-Event-Id, X-Event-Occurred-At
       성공 → markPublished()
       실패 → markFailed() retryCount++, 최대 5회 후 에러 로그
```

**AFTER_COMMIT을 쓰는 이유**
`@TransactionalEventListener`의 기본값은 `BEFORE_COMMIT`이다. 만약 여기서 outbox를 저장했는데 이후 트랜잭션이 롤백되면 outbox 레코드만 남고 비즈니스 데이터는 사라진다. `AFTER_COMMIT`으로 설정하면 비즈니스 트랜잭션이 완전히 커밋된 후에만 outbox가 저장된다.

**@Async를 쓰는 이유**
`AFTER_COMMIT` 리스너는 동기로 실행하면 원래 트랜잭션 스레드를 계속 점유한다. Tomcat 스레드를 빨리 반환하기 위해 비동기로 처리한다. outbox 저장 실패는 로그로 남기고, 스케줄러에 의한 재시도 경로는 없으므로 실패 시 해당 이벤트는 유실된다. (트레이드오프 — outbox 저장 자체가 실패할 확률은 낮지만, 완전한 보장을 원하면 동기 처리로 전환 가능)

### 3-2. 상품 조회 (Outbox 미사용, 직접 발행)

```
ProductFacade.getProductWithStock()
  └─ publishEvent(UserActionEvent(PRODUCT_VIEWED, null, productId))

UserActionEventListener.handleProductViewed()   @EventListener + @Async
  └─ kafkaTemplate.send("catalog-view-events-v1", null, payload)
       partitionKey = null (round-robin)
```

**Outbox를 쓰지 않는 이유**
조회 이벤트(`view_count`)는 누적 카운터이므로 순서가 보장될 필요가 없다. 또한 조회는 비즈니스 트랜잭션 없이 일어나므로 `@TransactionalEventListener`가 동작하지 않는다. 유실되더라도 통계 정확도에 약간의 오차가 생길 뿐 치명적이지 않다. Outbox를 쓰면 테이블 write 부하가 크게 늘어 조회 성능에 역효과를 준다.

**토픽을 분리한 이유**
주문/좋아요 이벤트(`catalog-events-v1`)와 분리한 이유는 두 가지다.

- **볼륨**: 조회 이벤트는 주문·좋아요 대비 압도적으로 많다. 같은 토픽에 넣으면 consumer 처리 병목이 생긴다.
- **retention**: `retention.bytes` 한도에 닿을 때 조회 이벤트가 오래된 주문/좋아요 이벤트를 밀어내는 현상을 막는다.

### 3-3. 쿠폰 발급 요청 (Outbox 미사용, 직접 발행)

```
CouponIssueFacade.requestIssue()
  └─ 유효성 검증 (만료 여부)
  └─ kafkaTemplate.send("coupon-issue-requests", couponId, payload)
       header: X-Event-Type: CouponIssueRequested
       partitionKey: couponId.toString()  (같은 쿠폰은 같은 파티션)
```

**partitionKey = couponId인 이유**
같은 쿠폰에 대한 발급 요청은 반드시 순서대로 처리되어야 중복 발급·초과 발급을 막을 수 있다. Kafka는 같은 파티션 내에서만 순서를 보장하므로 couponId를 키로 쓴다.

**CouponIssueProcessor 처리 흐름**

```
CouponIssueConsumer → CouponIssueProcessor.process()
  1. 중복 발급 체크: issuedCouponJpaRepository.existsByCouponIdAndUserId()
  2. Redis 재고 체크: DECR coupon:stock:{couponId}
       - 재고 없으면 INCR로 원복 후 return
       - key 없으면 무제한 발급
  3. DB 저장: IssuedCouponEntity 저장 + issuedCount 증가
```

Redis 재고 체크가 먼저이므로 DB에 락 없이 고속 처리가 가능하다. Redis DECR은 원자적(atomic)이므로 동시성 이슈가 없다.

---

## 4. Kafka 설정 파일 해설

### 4-1. `modules/kafka/src/main/resources/kafka.yml`

```yaml
spring.json.add.type.headers: false
```
Spring Kafka가 기본적으로 Java 클래스 타입 정보를 헤더에 넣는데, 이걸 켜두면 컨슈머가 같은 클래스 경로를 공유해야 한다. 멀티모듈 구조에서 `commerce-api`와 `commerce-streamer`는 공유 클래스가 없으므로 꺼야 한다.

```yaml
request.timeout.ms: 20000
retry.backoff.ms: 500
```
브로커 응답 대기 20초. 응답이 없으면 500ms 후 재시도. 프로덕션에서 브로커가 일시적으로 느릴 때 즉시 실패하지 않도록 하는 버퍼다.

```yaml
auto.create.topics.enable: false
```
브로커 사이드에서 토픽 자동 생성을 끈다. 오타나 잘못된 토픽명으로 메시지가 사라지는 사고를 막는다. 토픽은 `KafkaConfig`의 `@Bean NewTopic`으로만 생성한다.

```yaml
auto.offset.reset: latest
```
컨슈머 그룹이 처음 시작하거나 오프셋이 없을 때 가장 최신 메시지부터 읽는다. `earliest`로 설정하면 토픽에 쌓인 모든 과거 메시지를 처음부터 읽어 불필요한 재처리가 생긴다.

```yaml
producer.acks: all
```
프로듀서가 메시지를 보내면 리더 브로커 + 모든 팔로워가 복제 완료할 때까지 ACK를 기다린다. 브로커 장애 시에도 메시지 유실을 막는 가장 강한 내구성 설정이다.

```yaml
producer.retries: 3
producer.enable.idempotence: true
producer.delivery.timeout.ms: 70000
```
`retries: 3`은 일시적 네트워크 오류에 대한 재시도다. `enable.idempotence: true`와 함께 쓰면 재시도로 인한 중복 발행을 방지한다. `delivery.timeout.ms: 70000`은 첫 시도부터 최종 성공/실패 판정까지 최대 70초를 허용한다. (`request.timeout.ms` 20s × 재시도 횟수를 초과해야 하므로 70s로 설정)

```yaml
producer.compression-type: snappy
```
Snappy 압축은 CPU 사용률 대비 압축률이 좋다. 대용량 payload를 다루는 이 시스템에서 네트워크 대역폭을 줄이면서 압축·압축해제 오버헤드를 최소화하는 균형점이다.

```yaml
consumer.enable-auto-commit: false
listener.ack-mode: manual
```
자동 커밋을 끄고 수동 ACK를 쓴다. 자동 커밋은 메시지를 가져온 직후 오프셋을 커밋하므로, 처리 도중 실패해도 이미 커밋되어 재처리가 안 된다. 수동 커밋은 비즈니스 로직이 완전히 끝난 후 `ack.acknowledge()`를 호출할 때만 오프셋이 커밋된다.

### 4-2. `modules/kafka/src/main/java/com/loopers/confg/kafka/KafkaConfig.java`

**토픽 정의**

| 토픽 | 파티션 | 복제본 | 용도 |
|---|---|---|---|
| `catalog-events-v1` | 3 | 1 | 주문·좋아요·좋아요취소 이벤트 |
| `catalog-view-events-v1` | 3 | 1 | 상품 조회 이벤트 |
| `coupon-issue-requests` | 3 | 1 | 쿠폰 발급 요청 |
| `catalog-events-v1.dlq` | 1 | 1 | catalog-events-v1 처리 실패 격리 |
| `catalog-view-events-v1.dlq` | 1 | 1 | catalog-view-events-v1 처리 실패 격리 |
| `coupon-issue-requests.dlq` | 1 | 1 | coupon-issue-requests 처리 실패 격리 |

**메인 토픽 파티션 3개인 이유**
consumer concurrency를 3으로 설정했을 때 파티션이 3개여야 병렬 처리가 의미 있다. 파티션 수 < concurrency면 일부 스레드가 놀고, 파티션 수 > concurrency면 한 스레드가 여러 파티션을 담당해 병렬 효과가 줄어든다.

**DLQ 토픽 파티션 1개인 이유**
DLQ는 처리 실패한 메시지를 격리하는 용도다. 처리량이 적고 순서 보장보다 단순 보관이 목적이므로 파티션을 늘릴 이유가 없다. 파티션이 많으면 오히려 모니터링/재처리 시 여러 파티션을 따라가야 해서 불편하다.

**DLQ 토픽을 `@Bean`으로 명시한 이유**
`kafka.yml`에 `auto.create.topics.enable: false`로 설정되어 있어 브로커가 존재하지 않는 토픽으로 메시지를 수신해도 자동 생성하지 않는다. DLQ 토픽을 미리 선언하지 않으면 `DlqPublisher`가 처음 격리 메시지를 보낼 때 `UnknownTopicOrPartitionException`이 발생한다. `@Bean NewTopic`으로 애플리케이션 기동 시점에 미리 생성해두는 것이 안전하다.

**복제본 1개인 이유**
로컬/테스트 환경에서 단일 브로커로 실행하므로 복제본을 늘리면 브로커가 부족해 토픽 생성이 실패한다.

**BATCH_LISTENER 컨테이너 팩토리**

```java
public static final int MAX_POLLING_SIZE = 3000;
public static final int FETCH_MIN_BYTES = (1024 * 1024); // 1MB
public static final int FETCH_MAX_WAIT_MS = 5 * 1000;   // 5s
public static final int SESSION_TIMEOUT_MS = 60 * 1000; // 60s
public static final int HEARTBEAT_INTERVAL_MS = 20 * 1000; // 20s (1/3 of session timeout)
public static final int MAX_POLL_INTERVAL_MS = 2 * 60 * 1000; // 120s
```

- `MAX_POLL_RECORDS_CONFIG = 3000`: 한 번의 poll에서 최대 3000개 메시지를 가져온다. 조회 이벤트처럼 볼륨이 많은 경우 배치 단위로 묶어 처리하면 DB 왕복 횟수를 줄인다.

- `FETCH_MIN_BYTES = 1MB`: 브로커에서 최소 1MB가 쌓이거나 `FETCH_MAX_WAIT_MS`가 지나야 응답한다. 메시지가 드문드문 들어올 때 네트워크 왕복을 줄인다.

- `FETCH_MAX_WAIT_MS = 5s`: 1MB가 안 모여도 5초면 응답한다. 실시간성과 배치 효율의 균형점이다.

- `SESSION_TIMEOUT_MS = 60s`: 브로커가 이 시간 동안 heartbeat를 받지 못하면 컨슈머가 죽었다고 판단하고 리밸런싱을 시작한다. 60초는 GC 정지나 일시적 부하에 의한 오탐 리밸런싱을 방지하는 충분한 여유다.

- `HEARTBEAT_INTERVAL_MS = 20s`: SESSION_TIMEOUT의 1/3로 설정하는 것이 Kafka 공식 권장값이다. 20s 간격으로 heartbeat를 보내면 60s 안에 최소 2번은 도달한다.

- `MAX_POLL_INTERVAL_MS = 120s`: poll() 호출 사이 최대 허용 시간이다. 3000개 메시지를 처리하는 데 시간이 걸릴 수 있으므로 session timeout(60s)보다 넉넉하게 2분으로 잡았다. 이 시간을 초과하면 브로커는 해당 컨슈머가 멈췄다고 판단해 파티션을 다른 컨슈머에게 넘긴다.

- `setConcurrency(3)`: 리스너 컨테이너 내에서 3개의 스레드가 병렬로 파티션을 처리한다. 파티션 수(3)와 일치시켰다.

- `setBatchListener(true)`: 개별 메시지가 아닌 `List<ConsumerRecord>` 단위로 리스너가 호출된다. 한 번에 여러 메시지를 받아 처리한 뒤 한 번만 ACK한다.

- `AckMode.MANUAL`: 리스너 안에서 `acknowledgment.acknowledge()`를 직접 호출해야 오프셋이 커밋된다.

---

## 5. Outbox 구성 요소 상세

### 5-1. `OutboxEvent` (도메인 모델)

| 필드 | 설명 |
|---|---|
| `eventId` | UUID — consumer 측 멱등 키. 같은 이벤트가 중복 발행됐을 때 `EventHandledEntity`로 중복 처리를 막는다. |
| `eventType` | 이벤트 종류 (예: `OrderItemSoldEvent`). Kafka 헤더 `X-Event-Type`으로 전달된다. |
| `topicName` | 어느 토픽으로 발행할지. outbox 레코드마다 독립적으로 관리할 수 있다. |
| `partitionKey` | Kafka 파티션 키. 같은 키는 항상 같은 파티션으로 가므로 순서 보장이 필요한 이벤트에 사용한다. |
| `status` | `PENDING` → `PUBLISHED` / `FAILED` |
| `retryCount` | 발행 실패 누적 횟수. 5회 초과 시 에러 로그 발행 후 더 이상 재시도 안 함. |

### 5-2. `OutboxJpaRepository.findPendingWithSkipLocked()`

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
@Query("SELECT o FROM OutboxEvent o WHERE o.status = 'PENDING' OR (o.status = 'FAILED' AND o.retryCount < 5) ORDER BY o.id ASC LIMIT 500")
```

- `PESSIMISTIC_WRITE`: `SELECT ... FOR UPDATE`로 선택된 레코드를 잠근다.
- `lock.timeout = -2`: `-2`는 SKIP LOCKED를 의미한다. 잠긴 레코드는 건너뛰고 잠그지 않은 레코드만 가져온다. 다중 인스턴스 배포 시 스케줄러 여러 개가 동시에 같은 PENDING 이벤트를 가져가는 중복 발행 문제를 방지한다.
- `LIMIT 500`: 한 번에 최대 500개. 너무 많으면 트랜잭션 시간이 길어지고 락 경합이 심해진다.

### 5-3. `OutboxEventPublishScheduler`

```java
@Scheduled(fixedDelay = 1_000)
@Transactional
public void publish()
```

- `fixedDelay = 1s`: 이전 실행이 끝난 후 1초 뒤에 다음 실행이 시작된다. `fixedRate`는 이전 실행이 안 끝나도 다음이 시작되므로 동시 실행 가능성이 있다. `fixedDelay`가 안전하다.
- `kafkaTemplate.send(record).get()`: 블로킹 발행. 성공/실패를 확인 후 outbox 상태를 업데이트한다. 비동기(`send()`만)로 하면 발행 성공 여부를 모르는 상태에서 PUBLISHED로 마킹하는 실수가 생긴다.

---

## 6. 소비자 측 멱등 처리 (`CatalogMetricsProcessor`)

Kafka는 at-least-once 전달을 보장한다. 같은 메시지가 두 번 이상 전달될 수 있으므로 consumer 측에서 중복을 처리해야 한다.

```java
if (eventHandledJpaRepository.existsByEventId(eventId)) {
    return; // 이미 처리된 이벤트면 skip
}
// 처리 후
eventHandledJpaRepository.save(new EventHandledEntity(eventId, topic));
```

`eventId`는 Outbox에서 생성한 UUID로, Kafka 헤더 `X-Event-Id`로 전달된다. `EventHandledEntity`에 저장해두면 재처리 요청이 와도 중복 실행을 막는다.

---

## 7. DLQ (Dead Letter Queue)

```java
public void sendToDlq(ConsumerRecord<Object, Object> record, Exception cause) {
    String dlqTopic = record.topic() + ".dlq";
    kafkaTemplate.send(dlqTopic, record.key(), record.value());
}
```

처리 실패한 메시지는 원래 토픽에 재시도하지 않고 `{topic}.dlq` 토픽으로 격리한다. 무한 재시도로 인한 consumer lag 증가를 막기 위해서다. DLQ 토픽은 별도 모니터링 대상으로 관리한다.

DLQ 토픽은 `KafkaConfig`에서 `@Bean NewTopic`으로 명시적으로 선언되어 있다(`auto.create.topics.enable: false` 설정 때문에 선언 없이는 발행 자체가 실패한다).

적용 대상:
- `CatalogMetricsConsumer` → `catalog-events-v1.dlq`
- `CouponIssueConsumer` → `coupon-issue-requests.dlq`

`CatalogViewConsumer`는 view_count 누락이 치명적이지 않으므로 DLQ 대신 경고 로그만 남긴다. `catalog-view-events-v1.dlq`는 토픽만 선언되어 있고 현재 사용하는 consumer는 없다.

---

## 8. 전체 토픽 목록

| 토픽 | 파티션 | 프로듀서 | 컨슈머 | 파티션 키 | 비고 |
|---|---|---|---|---|---|
| `catalog-events-v1` | 3 | OutboxEventPublishScheduler | CatalogMetricsConsumer | orderId / userId | Outbox 경유 |
| `catalog-view-events-v1` | 3 | UserActionEventListener | CatalogViewConsumer | null (round-robin) | 직접 발행 |
| `coupon-issue-requests` | 3 | CouponIssueFacade | CouponIssueConsumer | couponId | 직접 발행, 순서 중요 |
| `catalog-events-v1.dlq` | 1 | DlqPublisher | (수동 모니터링) | - | 처리 실패 격리 |
| `catalog-view-events-v1.dlq` | 1 | (미사용) | (수동 모니터링) | - | 토픽만 선언, consumer 없음 |
| `coupon-issue-requests.dlq` | 1 | DlqPublisher | (수동 모니터링) | - | 처리 실패 격리 |
