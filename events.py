EVENT_TYPES = ["job_view", "save", "apply", "form_submit"]


def create_event(data: dict) -> dict:
    # For validating Purpose we cant directly give the event from event generator it may have some missing areas
    event_id = data.get("eventId")
    tenant_id = data.get("tenantId")
    event_type = data.get("eventType")

    if not event_id or not tenant_id or not event_type:
        raise ValueError("eventId, tenantId, eventType are required")

    if event_type not in EVENT_TYPES:
        raise ValueError(f"Unknown eventType: {event_type}")

    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()

    return {
        "eventId": event_id,
        "tenantId": tenant_id,
        "eventType": event_type,
        "schemaVersion": data.get("schemaVersion", 1),
        "occurredAt": data.get("occurredAt") or now,
        "receivedAt": now,
        "payload": data.get("payload") or {},
    }
