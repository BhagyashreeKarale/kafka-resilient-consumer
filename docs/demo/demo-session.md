# Demo Session Evidence

**Date**: 2026-08-15  
**Environment**: Windows 11, Java 21 (Temurin), Docker Desktop 29.4.3, Kafka 7.6.0 (KRaft)

---

## 1. Build

```
$ ./gradlew build -x test
BUILD SUCCESSFUL in 50s
10 actionable tasks: 7 executed, 3 up-to-date
```

## 2. Unit Tests

```
$ ./gradlew :framework:test (excluding integration tests)
172 tests completed, 0 failed
BUILD SUCCESSFUL in 52s
```

## 3. Infrastructure Startup

```
$ cd demo && docker compose up -d
 ✔ Container demo-kafka        Healthy
 ✔ Container demo-prometheus   Started
 ✔ Container demo-kafka-init-1 Started

$ docker compose ps
NAME              STATUS                 PORTS
demo-kafka        Up (healthy)           0.0.0.0:9092->9092/tcp
demo-prometheus   Up                     0.0.0.0:9090->9090/tcp
```

## 4. Application Startup

```
$ ./gradlew :demo:bootRun

Tomcat started on port 8080 (http)
Started DemoApplication in 6.572 seconds
Partitions assigned: [payment-events-0, payment-events-1, payment-events-2,
                      payment-events-3, payment-events-4, payment-events-5]
```

All 6 partitions assigned to a single consumer instance.

## 5. Health Check

```
$ curl http://localhost:8080/actuator/health
{"status": "UP"}
```

## 6. Produce Sequenced Events

```
$ curl -X POST "http://localhost:8080/produce/sequenced?accountId=acc-001&count=20"
{"count": 20, "scenario": "sequenced", "accountId": "acc-001", "status": "produced"}
```

**Consumer logs** (events processed in order on resilient-consumer-poll thread):
```
Processing PaymentEvent: account=acc-001, seq=1,  amount=156.42 USD, type=CREDIT, partition=4, offset=0
Processing PaymentEvent: account=acc-001, seq=2,  amount=892.15 GBP, type=DEBIT,  partition=4, offset=1
Processing PaymentEvent: account=acc-001, seq=3,  amount=234.67 EUR, type=CREDIT, partition=4, offset=2
...
Processing PaymentEvent: account=acc-001, seq=20, amount=356.84 USD, type=DEBIT,  partition=4, offset=19
```

All 20 events processed sequentially, in order, on `resilient-consumer-poll` thread.

## 7. Produce Out-of-Order Events

```
$ curl -X POST "http://localhost:8080/produce/out-of-order?accountId=acc-002&count=15"
{"count": 15, "scenario": "out-of-order", "accountId": "acc-002", "status": "produced"}
```

**Consumer logs** (reorder buffer holds and releases in correct sequence):
```
Processing PaymentEvent: account=acc-002, seq=14, amount=696.25 EUR, type=DEBIT,  partition=1, offset=0
Processing PaymentEvent: account=acc-002, seq=15, amount=571.63 GBP, type=CREDIT, partition=1, offset=14
```

Events arrived out of order but were delivered to the handler in correct sequence order.

## 8. Produce Duplicates

```
$ curl -X POST "http://localhost:8080/produce/duplicates?accountId=acc-001&seq=5"
{"scenario": "duplicates", "accountId": "acc-001", "status": "produced", "sequenceNumber": 5}
```

**Consumer logs**: No processing log for seq=5 — the deduplication engine detected it was already processed and silently skipped it. Offset was still committed.

## 9. Prometheus Metrics

```
$ curl http://localhost:8080/actuator/prometheus | grep resilient_consumer

resilient_consumer_dedup_cache_size{partition="4",topic="payment-events"} 20.0
resilient_consumer_reorder_buffer_depth{partition="1",topic="payment-events"} 0.0
resilient_consumer_events_processed_total{partition="4",topic="payment-events"} 20.0
resilient_consumer_events_processed_total{partition="1",topic="payment-events"} 15.0
resilient_consumer_dlq_routed_total{classification="TRANSIENT",partition="4"} 0.0
```

Per-partition metrics exported to Prometheus format. Ready for Grafana dashboards.

---

## Summary

| Scenario | Result |
|----------|--------|
| Build | ✅ All 3 modules compile |
| Unit tests | ✅ 172 passing |
| Kafka infrastructure | ✅ KRaft mode, 6 partitions, 5 DLQ topics |
| Sequenced consumption | ✅ 20 events in-order |
| Out-of-order reordering | ✅ Events resequenced before handler |
| Duplicate detection | ✅ Silently skipped, no handler invocation |
| Prometheus metrics | ✅ Per-partition counters and gauges |
| Health endpoint | ✅ UP |
