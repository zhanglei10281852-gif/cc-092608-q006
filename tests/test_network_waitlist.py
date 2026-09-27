from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.core.clock import FrozenClock, to_storage
from app.database import close_connection, get_connection
from app.network.operations import NetworkOperationsService
from app.network.rules import DEFAULT_RULES
from app.network.service import NetworkAccelerationService

T0 = datetime(2026, 9, 27, 8, 0, tzinfo=UTC)

SAMPLE_METRICS = {
    ("video-call", "critical"): {"latency_ms": 350, "packet_loss": 0.08, "downlink_mbps": 1.5, "uplink_mbps": 0.5},
    ("video-call", "major"): {"latency_ms": 280, "packet_loss": 0.035, "downlink_mbps": 3, "uplink_mbps": 1.5},
    ("video-call", "minor"): {"latency_ms": 150, "packet_loss": 0.015, "downlink_mbps": 6, "uplink_mbps": 3},
    ("office-work", "critical"): {"latency_ms": 800, "packet_loss": 0.3, "downlink_mbps": 1, "uplink_mbps": 0.2},
    ("office-work", "minor"): {"latency_ms": 260, "packet_loss": 0.06, "downlink_mbps": 3, "uplink_mbps": 0.8},
}


def prepare_metro(client, *, segment_capacity=16, rules=None):
    scenario = client.post(
        "/api/network/scenarios",
        json={"code": "metro-peak", "name": "地铁一号线早高峰", "scene_type": "metro", "timezone": "Asia/Shanghai", "max_concurrent_sessions": 10, "capacity_mbps": 1000},
    )
    assert scenario.status_code == 201, scenario.text
    segment = client.post(
        "/api/network/scenarios/metro-peak/segments",
        json={"code": "central", "name": "中心站区段", "sequence_no": 1, "expected_dwell_seconds": 180, "capacity_mbps": segment_capacity},
    )
    assert segment.status_code == 201, segment.text
    video_call = client.post(
        "/api/network/applications",
        json={"app_code": "video-call", "name": "视频通话", "category": "video_call", "latency_target_ms": 100, "packet_loss_target": 0.01, "min_downlink_mbps": 8, "min_uplink_mbps": 4, "default_priority": 70},
    )
    assert video_call.status_code == 201, video_call.text
    office = client.post(
        "/api/network/applications",
        json={"app_code": "office-work", "name": "移动办公", "category": "office", "latency_target_ms": 200, "packet_loss_target": 0.05, "min_downlink_mbps": 4, "min_uplink_mbps": 1, "default_priority": 30},
    )
    assert office.status_code == 201, office.text
    policy = client.post("/api/network/scenarios/metro-peak/policies", json={"rules": rules or DEFAULT_RULES, "actor": "tests"})
    assert policy.status_code == 201, policy.text
    published = client.post(f"/api/network/policies/{policy.json()['id']}/publish", json={"actor": "tests", "effective_from": "2026-01-01T00:00:00Z"})
    assert published.status_code == 200, published.text


def add_entitlement(client, subscriber, tier=50, valid_until="2030-01-01T00:00:00Z"):
    response = client.post(
        "/api/network/entitlements",
        json={
            "subscriber_hash": subscriber,
            "scenario_code": "metro-peak",
            "product_code": "metro-boost",
            "tier_level": tier,
            "valid_from": "2026-01-01T00:00:00Z",
            "valid_until": valid_until,
            "source_order_id": f"order-{subscriber}",
        },
    )
    assert response.status_code == 201, response.text


def make_incident(client, subscriber, app_code, severity, key):
    response = client.post(
        "/api/network/samples",
        json={
            "sample_key": key,
            "scenario_code": "metro-peak",
            "segment_code": "central",
            "app_code": app_code,
            "subscriber_hash": subscriber,
            "device_class": "phone",
            "train_speed_kmh": 40,
            "observed_at": "2026-09-27T07:55:00Z",
            **SAMPLE_METRICS[(app_code, severity)],
        },
    )
    assert response.status_code == 202, response.text
    incident_id = response.json()["incident_id"]
    assert incident_id is not None
    row = get_connection().execute("SELECT severity FROM quality_incidents WHERE id=?", (incident_id,)).fetchone()
    assert row["severity"] == severity
    return incident_id


def service_at(moment):
    return NetworkAccelerationService(get_connection(), FrozenClock(moment))


def scope_ids():
    connection = get_connection()
    scenario_id = connection.execute("SELECT id FROM network_scenarios WHERE code='metro-peak'").fetchone()[0]
    segment_id = connection.execute("SELECT id FROM network_segments WHERE scenario_id=? AND code='central'", (scenario_id,)).fetchone()[0]
    return scenario_id, segment_id


def test_waitlist_orders_by_explainable_factors(client):
    prepare_metro(client, segment_capacity=16)
    service = service_at(T0)
    add_entitlement(client, "subscriber-wl-a00001", tier=50)
    add_entitlement(client, "subscriber-wl-b00002", tier=90)
    add_entitlement(client, "subscriber-wl-c00003", tier=10)
    incident_a = make_incident(client, "subscriber-wl-a00001", "video-call", "critical", "wl-sample-a1")
    incident_b = make_incident(client, "subscriber-wl-b00002", "video-call", "major", "wl-sample-b1")
    incident_c = make_incident(client, "subscriber-wl-c00003", "office-work", "minor", "wl-sample-c1")
    started = service.start_acceleration(incident_a, "tests")
    assert started["queued"] is False
    queued_b = service.start_acceleration(incident_b, "tests")
    assert queued_b["queued"] is True
    assert queued_b["reason"] == "capacity"
    queued_c = service.start_acceleration(incident_c, "tests")
    entry_b = queued_b["entry"]
    entry_c = queued_c["entry"]
    assert entry_b["rank"] == 1
    assert entry_c["rank"] == 2
    factors_b = entry_b["factors"]
    assert factors_b["severity"] == "major"
    assert factors_b["severity_score"] == 2000
    assert factors_b["app_priority_score"] == 700
    assert factors_b["tier_score"] == 450
    assert factors_b["aging_score"] == 0
    assert factors_b["concurrency_penalty"] == 0
    assert factors_b["total_score"] == 3150
    assert entry_c["factors"]["total_score"] == 1350
    again = service.start_acceleration(incident_b, "tests")
    assert again["queued"] is True
    assert again["duplicate"] is True
    assert again["entry"]["id"] == entry_b["id"]
    waiting = service.waitlist_entries("metro-peak")
    assert [item["id"] for item in waiting] == [entry_b["id"], entry_c["id"]]


def test_multiple_releases_promote_without_oversell_or_duplicates(client):
    prepare_metro(client, segment_capacity=16)
    service = service_at(T0)
    add_entitlement(client, "subscriber-wl-a00001", tier=50)
    add_entitlement(client, "subscriber-wl-b00002", tier=90)
    add_entitlement(client, "subscriber-wl-c00003", tier=10)
    incident_a = make_incident(client, "subscriber-wl-a00001", "video-call", "critical", "wl-sample-a2")
    incident_b = make_incident(client, "subscriber-wl-b00002", "video-call", "major", "wl-sample-b2")
    incident_c = make_incident(client, "subscriber-wl-c00003", "office-work", "minor", "wl-sample-c2")
    started = service.start_acceleration(incident_a, "tests")
    entry_b = service.start_acceleration(incident_b, "tests")["entry"]
    entry_c = service.start_acceleration(incident_c, "tests")["entry"]
    scenario_id, segment_id = scope_ids()

    def held():
        return service.repository.active_capacity(scenario_id, segment_id)

    first = service.finish_session(started["id"], "tests", "体验恢复", "completed")
    assert first["promoted_waitlist_entries"] == [entry_b["id"]]
    assert held()["downlink_mbps"] == pytest.approx(12.0)
    assert held()["sessions"] == 1
    assert held()["downlink_mbps"] <= 16
    waiting_c = service.waitlist_entry(entry_c["id"])
    assert waiting_c["state"] == "waiting"
    assert waiting_c["rank"] == 1
    promoted_b = service.waitlist_entry(entry_b["id"])
    assert promoted_b["state"] == "promoted"
    assert promoted_b["session_id"] is not None
    second = service.finish_session(promoted_b["session_id"], "tests", "体验恢复", "completed")
    assert second["promoted_waitlist_entries"] == [entry_c["id"]]
    assert held()["downlink_mbps"] == pytest.approx(4.6)
    assert held()["downlink_mbps"] <= 16
    promoted_c = service.waitlist_entry(entry_c["id"])
    assert promoted_c["state"] == "promoted"
    connection = get_connection()
    duplicates = connection.execute("SELECT incident_id,COUNT(*) AS amount FROM acceleration_sessions GROUP BY incident_id HAVING amount>1").fetchall()
    assert duplicates == []
    sessions = connection.execute("SELECT COUNT(*) FROM acceleration_sessions").fetchone()[0]
    assert sessions == 3
    accelerating = connection.execute("SELECT COUNT(*) FROM quality_incidents WHERE state='accelerating'").fetchone()[0]
    assert accelerating == 1


def test_aging_lets_long_waiting_low_priority_win(client):
    prepare_metro(client, segment_capacity=16)
    clock = FrozenClock(T0)
    service = NetworkAccelerationService(get_connection(), clock)
    add_entitlement(client, "subscriber-wl-a00001", tier=50)
    add_entitlement(client, "subscriber-wl-low004", tier=0)
    add_entitlement(client, "subscriber-wl-high05", tier=100)
    incident_a = make_incident(client, "subscriber-wl-a00001", "video-call", "critical", "wl-sample-a3")
    incident_low = make_incident(client, "subscriber-wl-low004", "office-work", "minor", "wl-sample-l3")
    incident_high = make_incident(client, "subscriber-wl-high05", "video-call", "critical", "wl-sample-h3")
    started = service.start_acceleration(incident_a, "tests")
    entry_low = service.start_acceleration(incident_low, "tests")["entry"]
    clock.advance(minutes=150)
    entry_high = service.start_acceleration(incident_high, "tests")["entry"]
    waiting = service.waitlist_entries("metro-peak")
    assert [item["id"] for item in waiting] == [entry_low["id"], entry_high["id"]]
    assert waiting[0]["factors"]["aging_score"] == 3000
    assert waiting[0]["factors"]["total_score"] == 4300
    assert waiting[1]["factors"]["aging_score"] == 0
    assert waiting[1]["factors"]["total_score"] == 4200
    finished = service.finish_session(started["id"], "tests", "体验恢复", "completed")
    assert finished["promoted_waitlist_entries"] == [entry_low["id"]]
    assert service.waitlist_entry(entry_high["id"])["state"] == "waiting"
    promoted_low = service.waitlist_entry(entry_low["id"])
    assert promoted_low["factors"]["aging_score"] == 3000
    second = service.finish_session(promoted_low["session_id"], "tests", "体验恢复", "completed")
    assert second["promoted_waitlist_entries"] == [entry_high["id"]]
    assert service.waitlist_entry(entry_high["id"])["state"] == "promoted"


def test_session_expiry_triggers_waitlist_promotion(client):
    prepare_metro(client, segment_capacity=16)
    clock = FrozenClock(T0)
    service = NetworkAccelerationService(get_connection(), clock)
    add_entitlement(client, "subscriber-wl-a00001", tier=50)
    add_entitlement(client, "subscriber-wl-b00002", tier=90)
    incident_a = make_incident(client, "subscriber-wl-a00001", "video-call", "critical", "wl-sample-a4")
    incident_b = make_incident(client, "subscriber-wl-b00002", "video-call", "major", "wl-sample-b4")
    started = service.start_acceleration(incident_a, "tests")
    queued = service.start_acceleration(incident_b, "tests")
    clock.advance(seconds=181)
    result = service.expire_sessions("tests")
    assert result["expired"] == [started["id"]]
    assert result["promoted"] == [queued["entry"]["id"]]
    entry = service.waitlist_entry(queued["entry"]["id"])
    assert entry["state"] == "promoted"
    session = service.get_session(entry["session_id"])
    assert session["status"] == "active"
    assert session["allocated_downlink_mbps"] == pytest.approx(12.0)


def test_maintenance_completion_triggers_promotion(client):
    prepare_metro(client, segment_capacity=16)
    clock = FrozenClock(T0)
    connection = get_connection()
    operations = NetworkOperationsService(connection, clock)
    window = operations.create_maintenance({
        "scenario_code": "metro-peak",
        "segment_code": "central",
        "code": "peak-maintenance",
        "reason": "射频优化",
        "starts_at": to_storage(T0 - timedelta(minutes=10)),
        "ends_at": to_storage(T0 + timedelta(minutes=30)),
        "drain_mode": "block_new",
        "actor": "operator",
    })
    activated = operations.activate_due_maintenance("tests")
    assert window["id"] in activated["activated"]
    service = NetworkAccelerationService(connection, clock)
    add_entitlement(client, "subscriber-wl-b00002", tier=90)
    incident_b = make_incident(client, "subscriber-wl-b00002", "video-call", "major", "wl-sample-m5")
    queued = service.start_acceleration(incident_b, "tests")
    assert queued["queued"] is True
    assert queued["reason"] == "maintenance_window"
    clock.advance(minutes=31)
    completed = NetworkOperationsService(connection, clock).activate_due_maintenance("tests")
    assert window["id"] in completed["completed"]
    assert queued["entry"]["id"] in completed["promoted"]
    entry = service.waitlist_entry(queued["entry"]["id"])
    assert entry["state"] == "promoted"
    assert entry["session_id"] is not None


def test_restart_preserves_queue_order(client):
    prepare_metro(client, segment_capacity=16)
    clock = FrozenClock(T0)
    service = NetworkAccelerationService(get_connection(), clock)
    add_entitlement(client, "subscriber-wl-a00001", tier=50)
    add_entitlement(client, "subscriber-wl-b00002", tier=90)
    add_entitlement(client, "subscriber-wl-c00003", tier=10)
    add_entitlement(client, "subscriber-wl-d00004", tier=50)
    incident_a = make_incident(client, "subscriber-wl-a00001", "video-call", "critical", "wl-sample-a6")
    incident_b = make_incident(client, "subscriber-wl-b00002", "video-call", "major", "wl-sample-b6")
    incident_c = make_incident(client, "subscriber-wl-c00003", "office-work", "minor", "wl-sample-c6")
    incident_d = make_incident(client, "subscriber-wl-d00004", "video-call", "minor", "wl-sample-d6")
    service.start_acceleration(incident_a, "tests")
    entry_b = service.start_acceleration(incident_b, "tests")["entry"]
    clock.advance(minutes=1)
    entry_d = service.start_acceleration(incident_d, "tests")["entry"]
    clock.advance(minutes=1)
    entry_c = service.start_acceleration(incident_c, "tests")["entry"]
    before = [(item["id"], item["rank"], item["factors"]["total_score"]) for item in service.waitlist_entries("metro-peak")]
    assert [item[0] for item in before] == [entry_b["id"], entry_d["id"], entry_c["id"]]
    close_connection()
    reopened = NetworkAccelerationService(get_connection(), FrozenClock(T0 + timedelta(minutes=2)))
    after = [(item["id"], item["rank"], item["factors"]["total_score"]) for item in reopened.waitlist_entries("metro-peak")]
    assert before == after


def test_subscriber_concurrency_limit_queues_even_with_capacity(client):
    prepare_metro(client, segment_capacity=100)
    service = service_at(T0)
    add_entitlement(client, "subscriber-wl-s00006", tier=50)
    first_incident = make_incident(client, "subscriber-wl-s00006", "video-call", "major", "wl-sample-s1")
    second_incident = make_incident(client, "subscriber-wl-s00006", "video-call", "major", "wl-sample-s2")
    started = service.start_acceleration(first_incident, "tests")
    assert started["queued"] is False
    queued = service.start_acceleration(second_incident, "tests")
    assert queued["queued"] is True
    assert queued["reason"] == "subscriber_concurrency"
    finished = service.finish_session(started["id"], "tests", "体验恢复", "completed")
    assert finished["promoted_waitlist_entries"] == [queued["entry"]["id"]]
    assert service.waitlist_entry(queued["entry"]["id"])["state"] == "promoted"


def test_concurrency_penalty_shapes_order(client):
    rules = {**DEFAULT_RULES, "allocation": {**DEFAULT_RULES["allocation"], "max_active_per_subscriber": 2}}
    prepare_metro(client, segment_capacity=40, rules=rules)
    service = service_at(T0)
    add_entitlement(client, "subscriber-wl-s00007", tier=50)
    add_entitlement(client, "subscriber-wl-f00008", tier=50)
    add_entitlement(client, "subscriber-wl-w00009", tier=50)
    incident_s1 = make_incident(client, "subscriber-wl-s00007", "video-call", "critical", "wl-sample-p1")
    incident_f = make_incident(client, "subscriber-wl-f00008", "video-call", "critical", "wl-sample-p2")
    incident_s2 = make_incident(client, "subscriber-wl-s00007", "video-call", "major", "wl-sample-p3")
    incident_w = make_incident(client, "subscriber-wl-w00009", "video-call", "major", "wl-sample-p4")
    service.start_acceleration(incident_s1, "tests")
    filler = service.start_acceleration(incident_f, "tests")
    entry_s2 = service.start_acceleration(incident_s2, "tests")["entry"]
    entry_w = service.start_acceleration(incident_w, "tests")["entry"]
    waiting = service.waitlist_entries("metro-peak")
    assert [item["id"] for item in waiting] == [entry_w["id"], entry_s2["id"]]
    factors_s2 = waiting[1]["factors"]
    assert factors_s2["active_sessions"] == 1
    assert factors_s2["concurrency_penalty"] == 300
    assert waiting[0]["factors"]["concurrency_penalty"] == 0
    finished = service.finish_session(filler["id"], "tests", "体验恢复", "completed")
    assert finished["promoted_waitlist_entries"] == [entry_w["id"], entry_s2["id"]]


def test_entitlement_lapse_cancels_waiting_entry(client):
    prepare_metro(client, segment_capacity=16)
    clock = FrozenClock(T0)
    service = NetworkAccelerationService(get_connection(), clock)
    add_entitlement(client, "subscriber-wl-a00001", tier=50)
    add_entitlement(client, "subscriber-wl-b00002", tier=90, valid_until=to_storage(T0 + timedelta(minutes=10)))
    incident_a = make_incident(client, "subscriber-wl-a00001", "video-call", "critical", "wl-sample-a9")
    incident_b = make_incident(client, "subscriber-wl-b00002", "video-call", "major", "wl-sample-b9")
    started = service.start_acceleration(incident_a, "tests")
    queued = service.start_acceleration(incident_b, "tests")
    clock.advance(minutes=20)
    finished = service.finish_session(started["id"], "tests", "体验恢复", "completed")
    assert "promoted_waitlist_entries" not in finished
    entry = service.waitlist_entry(queued["entry"]["id"])
    assert entry["state"] == "cancelled"
    assert entry["cancel_reason"] == "entitlement_lapsed"
    scenario_id, segment_id = scope_ids()
    assert service.repository.active_capacity(scenario_id, segment_id)["sessions"] == 0


def test_waitlist_query_api(client):
    prepare_metro(client, segment_capacity=16)
    add_entitlement(client, "subscriber-wl-a00001", tier=50)
    add_entitlement(client, "subscriber-wl-b00002", tier=90)
    incident_a = make_incident(client, "subscriber-wl-a00001", "video-call", "critical", "wl-sample-aq")
    incident_b = make_incident(client, "subscriber-wl-b00002", "video-call", "major", "wl-sample-bq")
    started = client.post(f"/api/network/incidents/{incident_a}/accelerate", json={"actor": "tests"})
    assert started.status_code == 200
    assert started.json()["queued"] is False
    queued = client.post(f"/api/network/incidents/{incident_b}/accelerate", json={"actor": "tests"})
    assert queued.status_code == 200
    assert queued.json()["queued"] is True
    listing = client.get("/api/network/waitlist", params={"scenario_code": "metro-peak"})
    assert listing.status_code == 200
    items = listing.json()["items"]
    assert len(items) == 1
    assert items[0]["rank"] == 1
    assert items[0]["queue_reason"] == "capacity"
    assert set(items[0]["factors"]) >= {"severity_score", "app_priority_score", "tier_score", "aging_score", "concurrency_penalty", "total_score"}
    detail = client.get(f"/api/network/waitlist/{items[0]['id']}")
    assert detail.status_code == 200
    assert detail.json()["events"][0]["event_type"] == "enqueued"
    assert client.get("/api/network/waitlist", params={"state": "promoted"}).json()["items"] == []
    assert client.get("/api/network/waitlist", params={"state": "bogus"}).status_code == 422
    assert client.get("/api/network/waitlist/9999").status_code == 404
