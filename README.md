# Career Events Queue

Your part only: the middle mailbox between Event Generator and Event Consumer.

```
Career Site UI
      │
      ▼
Event Generator     ← builds the event
      │
      ▼
  YOUR QUEUE        ← this project (validate → Kafka → deliver)
      │
      ▼
Event Consumer      ← checks event, then add/edit in DB
```

You do **not** touch the database. You only move events safely.

---

## Flow (functions used)

```
Event Generator
      │  POST /events
      ▼
produce.py → post_event()
      │
      ├─ events.py → create_event()     validate + shape event
      │
      ├─ kafka_queue.py → get_producer()  Kafka sender (startup)
      │                 → enqueue()       put event on Kafka
      │
      └─ return 202 accepted / 400 error / 500 error
      ▼
Kafka topic: career-events
      │  worker reads message
      ▼
worker.py → main()
      │
      ├─ missing tenantId?
      │     └─ send_to_dlq() → topic career-events-dlq
      │
      ├─ handoff_to_consumer()  POST event to Event Consumer
      │     ├─ success → consumer.commit()  (done)
      │     └─ fail → retry up to 3 times (MAX_RETRIES)
      │
      └─ still failing?
            └─ send_to_dlq() → topic career-events-dlq
                  └─ producer.flush()  force write now
```

### Step by step — what is used

| Step | Function | File | Specifically used for |
|------|----------|------|------------------------|
| 1. Receive HTTP request | `post_event()` | `produce.py` | Entry point for Event Generator (`POST /events`) |
| 2. Validate event | `create_event()` | `events.py` | Checks `eventId`, `tenantId`, `eventType`; fills `schemaVersion`, `occurredAt`, `receivedAt`, `payload` |
| 3. Create Kafka sender | `get_producer()` | `kafka_queue.py` | Builds `KafkaProducer` connected to `localhost:9092` |
| 4. Put on queue | `enqueue()` | `kafka_queue.py` | Sends event to topic `career-events` with key = `tenantId` |
| 5. Reply to generator | `post_event()` return | `produce.py` | `202` accepted, or `400`/`500` with error |
| 6. Read from Kafka | `main()` + `KafkaConsumer` | `worker.py` | Listens on `career-events` using `GROUP_ID` |
| 7. Deliver to consumer | `handoff_to_consumer()` | `worker.py` | POST event to `CONSUMER_URL` |
| 8. Mark success | `consumer.commit()` | `worker.py` | Tells Kafka this message is done |
| 9. On failure | retry loop in `main()` | `worker.py` | Tries again up to 3 times with wait `2s / 4s / 8s` |
| 10. Give up | `send_to_dlq()` | `worker.py` | Writes failed event + error to `career-events-dlq`, then `flush()` |

---

## What this does 

1. Event Generator sends you an event (`save`, `apply`, `job_view`, etc.).
2. You **validate** it (must have `eventId`, `tenantId`, `eventType`).
3. If bad → return error.
4. If good → put it on **Kafka** and reply **accepted** with the `eventId`.
5. Worker picks the event from Kafka.
6. Worker sends it to Event Consumer (`handoff_to_consumer`).
7. Success → done.
8. Fail → retry up to **3 times**.
9. Still fail → send to **DLQ** (dead letter box) so it does not block other events.

---

## Input

Event Generator calls: `POST /events`

```json
{
  "eventId": "evt_1",
  "tenantId": "t1",
  "eventType": "apply",
  "schemaVersion": 1,
  "occurredAt": "2026-09-30T10:15:00Z",
  "payload": {
    "jobId": "j10",
    "userId": "u5"
  }
}
```

Allowed `eventType`: `job_view`, `save`, `apply`, `form_submit`

---

## Output

| Where | What |
|-------|------|
| Back to Event Generator | `202` + `{ "accepted": true, "eventId": "evt_1" }` |
| Bad event | `400` + `{ "accepted": false, "error": "..." }` |
| To Event Consumer | Same event JSON (POST) |
| After 3 failures | Event goes to DLQ topic `career-events-dlq` |

---

## Files

| File | What it does |
|------|----------------|
| `events.py` | Validate and shape the event |
| `kafka_queue.py` | Create Kafka producer and enqueue message |
| `produce.py` | HTTP API — receive event, validate, put on Kafka, return accepted/error |
| `worker.py` | Read from Kafka, handoff to consumer, retry 3 times, then DLQ |

---

## Important cases handled

| Case | What we do |
|------|------------|
| Bad / missing fields | Reject with 400 |
| Duplicate `eventId` | Same id can be used to avoid double work later |
| Tenant isolation | Kafka message key = `tenantId` |
| New fields later | Keep them in `payload` + `schemaVersion` |
| Late events | Still deliver; keep original `occurredAt` |
| Consumer fails | Retry 3 times, then DLQ |

---

## Not your job

- Career Site UI
- Event Generator logic
- Database add / edit
- Dashboard / analytics

Your job: **receive → validate → Kafka → deliver (or DLQ)**.
