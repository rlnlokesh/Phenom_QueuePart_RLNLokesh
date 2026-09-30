"""
Producer API — Event Generator calls this.

INPUT:  event JSON from Event Generator
OUTPUT: 202 accepted, and event is on Kafka topic
"""

import os

from flask import Flask, jsonify, request

from events import create_event
from kafka_queue import enqueue, get_producer

app = Flask(__name__)
producer = get_producer()


@app.post("/events")
def post_event(): 
    # receive the event validate it and then sends to kafka queue
    try:
        event = create_event(request.get_json(force=True))
        enqueue(producer, event)
        # sync reply to generator — real work continues async in Kafka
        return jsonify({"accepted": True, "eventId": event["eventId"]}), 202
    except ValueError as e:
        return jsonify({"accepted": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"accepted": False, "error": str(e)}), 500


@app.get("/health")
def health():
    return jsonify({"ok": True})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "3001"))
    print(f"Queue producer on :{port}")
    app.run(host="0.0.0.0", port=port)
