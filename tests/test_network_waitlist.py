from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.core.clock import FrozenClock, to_storage
from app.core.errors import ConflictError
from app.database import close_connection, get_connection
from app.network.operations import NetworkOperationsService
from app.network.rules import DEFAULT_RULES
from app.network.service import NetworkAccelerationService
from app.network.waitlist import WaitlistService

START = datetime(2026, 9, 27, 8, 0, tzinfo=UTC)


def scenario_payload(**overrides):
    payload = {
        "code": "metro-line-1",
        "name": "地铁一号线",
        "scene_type": "metro",
        "timezone": "Asia/Shanghai",
        "max_concurrent_sessions": 10,
        "capacity_mbps": 1000,
    }
    payload.update(overrides)
    return payload


def video_call_payload(**overrides):
    payload = {
        "app_code": "video-call",
        "name": "视频通话",
        "category": "video_call",
        "latency_target_ms": 100,
        "packet_loss_target": 0.01,
        "min_downlink_mbps": 8,
        "min_uplink_mbps": 4,
        "default_priority": 90,
    }
    payload.update(overrides)
    return payload


def office_payload(**overrides):
    payload = {
        "app_code": "office-sync",
        "name": "办公同步",
        "category": "office",
        "latency_target_ms": 300,
        "packet_loss_target": 0.05,
        "min_downlink_mbps": 4,
        "min_uplink_mbps": 2,
        "default_priority": 30,
    }
    payload.update(overrides)
    return payload


def sample_payload(**overrides):
    payload = {
        "sample_key": "sample-000001",
        "scenario_code": "metro-line-1",
        "segment_code": "rush-segment",
        "app_code": "video-call",
        "subscriber_hash": "subscriber-000000000001",
        "device_class": "phone",
        "train_speed_kmh": 80,
        "latency_ms": 350,
        "packet_loss": 0.08,
        "downlink_mbps": 1.5,
        "uplink_mbps": 0.5,
        "observed_at": "2026-09-27T00:00:00Z",
    }
    payload.update(overrides)
    return payload


def prepare_api(client, *, capacity_mbps=16, max_sessions=10):
    scenario = client.post("/api/network/scenarios", json=scenario_payload(max_concurrent_sessions=max_sessions))
    assert scenario.status_code == 201, scenario.text
    segment = client.post(
        "/api/network/scenarios/metro-line-1/segments",
        json={"code": "rush-segment", "name": "早高峰区段", "sequence_no": 1, "expected_dwell_seconds": 300, "capacity_mbps": capacity_mbps},
    )
    assert segment.status_code == 201, segment.text
    assert client.post("/api/network/applications", json=video_call_payload()).status_code == 201
    assert client.post("/api/network/applications", json=office_payload()).status_code == 201
    policy = client.post("/api/network/scenarios/metro-line-1/policies", json={"rules": DEFAULT_RULES, "actor": "tests"})
    assert policy.status_code == 201, policy.text
    published = client.post(
        f"/api/network/policies/{policy.json()['id']}/publish",
        json={"actor": "tests", "effective_from": "2020-01-01T00:00:00Z"},
    )
    assert published.status_code == 200, published.text
    assert client.post("/api/network/product-tiers", json={"product_code": "metro-basic", "tier": 0, "actor": "tests"}).status_code == 201
    assert client.post("/api/network/product-tiers", json={"product_code": "metro-premium", "tier": 3, "actor": "tests"}).status_code == 201


def add_entitlement_api(client, subscriber, product="metro-premium"):
    response = client.post(
        "/api/network/entitlements",
        json={
            "subscriber_hash": subscriber,
            "scenario_code": "metro-line-1",
            "product_code": product,
            "valid_from": "2020-01-01T00:00:00Z",
            "valid_until": "2030-01-01T00:00:00Z",
            "source_order_id": f"order-{subscriber}",
        },
    )
    assert response.status_code == 201, response.text


def ingest_api(client, *, sample_key, subscriber, app_code="video-call", **metrics):
    response = client.post("/api/network/samples", json=sample_payload(sample_key=sample_key, subscriber_hash=subscriber, app_code=app_code, **metrics))
    assert response.status_code == 202, response.text
    incident_id = response.json()["incident_id"]
    assert incident_id is not None
    return incident_id


def build_world(accel: NetworkAccelerationService, waitlist: WaitlistService, *, capacity_mbps=16, max_sessions=10):
    accel.create_scenario(scenario_payload(max_concurrent_sessions=max_sessions))
    accel.add_segment("metro-line-1", {"code": "rush-segment", "name": "早高峰区段", "sequence_no": 1, "expected_dwell_seconds": 300, "capacity_mbps": capacity_mbps})
    accel.create_application(video_call_payload())
    accel.create_application(office_payload())
    policy = accel.create_policy("metro-line-1", DEFAULT_RULES, "tests")
    accel.publish_policy(policy["id"], "tests", "2020-01-01T00:00:00Z")
    waitlist.upsert_product_tier({"product_code": "metro-basic", "tier": 0, "actor": "tests"})
    waitlist.upsert_product_tier({"product_code": "metro-premium", "tier": 3, "actor": "tests"})


def add_entitlement(accel: NetworkAccelerationService, subscriber, *, product="metro-basic", valid_from="2026-09-27T00:00:00Z", valid_until="2026-09-28T00:00:00Z"):
    return accel.add_entitlement(
        {
            "subscriber_hash": subscriber,
            "scenario_code": "metro-line-1",
            "product_code": product,
            "valid_from": valid_from,
            "valid_until": valid_until,
            "source_order_id": f"order-{subscriber}-{product}",
        }
    )


def make_incident(accel: NetworkAccelerationService, *, sample_key, subscriber, app_code="video-call", **metrics):
    result = accel.ingest_sample(sample_payload(sample_key=sample_key, subscriber_hash=subscriber, app_code=app_code, **metrics))
    assert result["incident_id"] is not None
    return result["incident_id"]


def held_downlink(connection) -> float:
    return float(connection.execute("SELECT COALESCE(SUM(downlink_mbps),0) FROM capacity_reservations WHERE state='held'").fetchone()[0])


def active_sessions(connection) -> list:
    return connection.execute("SELECT * FROM acceleration_sessions WHERE status='active' ORDER BY id").fetchall()


def test_enqueue_is_idempotent_and_exposes_rank_and_factors(client):
    prepare_api(client)
    first = "subscriber-000000000001"
    second = "subscriber-000000000002"
    third = "subscriber-000000000003"
    add_entitlement_api(client, first)
    add_entitlement_api(client, second)
    add_entitlement_api(client, third, product="metro-basic")
    incidents = {
        first: ingest_api(client, sample_key="sample-queue-1", subscriber=first),
        second: ingest_api(client, sample_key="sample-queue-2", subscriber=second),
        # 第三位用户是办公同步 minor 事件且为基础权益，静态分数低于视频通话 critical。
        third: ingest_api(
            client,
            sample_key="sample-queue-3",
            subscriber=third,
            app_code="office-sync",
            latency_ms=330,
            packet_loss=0.05,
            downlink_mbps=4,
            uplink_mbps=2,
        ),
    }
    promoted = client.post(f"/api/network/incidents/{incidents[first]}/waitlist", json={"actor": "tests"})
    assert promoted.status_code == 201, promoted.text
    assert promoted.json()["state"] == "promoted"
    assert promoted.json()["promoted_session_id"] is not None
    queued = client.post(f"/api/network/incidents/{incidents[second]}/waitlist", json={"actor": "tests"})
    assert queued.status_code == 201, queued.text
    assert queued.json()["state"] == "waiting"
    assert queued.json()["rank"] == 1
    replay = client.post(f"/api/network/incidents/{incidents[second]}/waitlist", json={"actor": "tests"})
    assert replay.status_code == 201
    assert replay.json()["id"] == queued.json()["id"]
    low = client.post(f"/api/network/incidents/{incidents[third]}/waitlist", json={"actor": "tests"})
    assert low.status_code == 201
    assert low.json()["rank"] == 2
    listing = client.get("/api/network/waitlist", params={"scenario_code": "metro-line-1"})
    assert listing.status_code == 200
    items = listing.json()["items"]
    assert len(items) == 2
    assert [item["rank"] for item in items] == [1, 2]
    assert [item["incident_id"] for item in items] == [incidents[second], incidents[third]]
    factors = items[0]["factors"]
    assert factors["severity"] == "critical"
    assert factors["severity_points"] == 300
    assert factors["app_priority"] == 90
    assert factors["entitlement_tier"] == 3
    assert factors["tier_points"] == 30
    assert factors["score"] == factors["severity_points"] + factors["app_priority_points"] + factors["tier_points"] + factors["wait_points"]
    assert factors["per_user_session_limit"] == 1
    assert factors["eligible"] is True
    low_factors = items[1]["factors"]
    assert low_factors["severity"] == "minor"
    assert low_factors["severity_points"] == 100
    assert low_factors["app_priority"] == 30
    assert low_factors["entitlement_tier"] == 0
    detail = client.get(f"/api/network/waitlist/{queued.json()['id']}")
    assert detail.status_code == 200
    assert detail.json()["rank"] == 1
    assert [event["event_type"] for event in detail.json()["events"]] == ["enqueued"]
    connection = get_connection()
    count = connection.execute("SELECT COUNT(*) FROM waitlist_entries WHERE incident_id=?", (incidents[second],)).fetchone()[0]
    assert count == 1


def test_enqueue_validates_incident_entitlement_and_state(client):
    prepare_api(client)
    missing = client.post("/api/network/incidents/9999/waitlist", json={"actor": "tests"})
    assert missing.status_code == 404
    subscriber = "subscriber-000000000010"
    incident = ingest_api(client, sample_key="sample-no-entitlement", subscriber=subscriber)
    denied = client.post(f"/api/network/incidents/{incident}/waitlist", json={"actor": "tests"})
    assert denied.status_code == 409
    bad_state = client.get("/api/network/waitlist", params={"state": "unknown"})
    assert bad_state.status_code == 422


def test_repeated_capacity_release_promotes_without_oversell_or_duplicate_sessions(client):
    prepare_api(client, capacity_mbps=16)
    subscribers = [f"subscriber-{index:018d}" for index in range(1, 5)]
    incidents = []
    for index, subscriber in enumerate(subscribers, start=1):
        add_entitlement_api(client, subscriber, product="metro-basic")
        incidents.append(ingest_api(client, sample_key=f"sample-release-{index:03d}", subscriber=subscriber))
    entries = []
    for incident in incidents:
        response = client.post(f"/api/network/incidents/{incident}/waitlist", json={"actor": "tests"})
        assert response.status_code == 201, response.text
        entries.append(response.json()["id"])
    connection = get_connection()
    assert entries[0] is not None
    assert client.get(f"/api/network/waitlist/{entries[0]}").json()["state"] == "promoted"
    # 容量 16 Mbps 每次只能容纳一个视频通话 critical 会话（16 Mbps）。
    for round_no, entry_id in enumerate(entries[1:], start=2):
        active = active_sessions(connection)
        assert len(active) == 1
        finished = client.post(
            f"/api/network/sessions/{active[0]['id']}/finish",
            json={"actor": "tests", "reason": f"第{round_no}次释放", "result": "completed"},
        )
        assert finished.status_code == 200, finished.text
        assert held_downlink(connection) == 16.0
        assert len(active_sessions(connection)) == 1
        detail = client.get(f"/api/network/waitlist/{entry_id}").json()
        assert detail["state"] == "promoted"
        assert detail["promoted_session_id"] == active_sessions(connection)[0]["id"]
        duplicates = connection.execute("SELECT incident_id,COUNT(*) AS c FROM acceleration_sessions GROUP BY incident_id HAVING c>1").fetchall()
        assert duplicates == []
        manual = client.post("/api/network/waitlist/promote", json={"actor": "tests"})
        assert manual.status_code == 200
        assert manual.json() == {"promoted": [], "expired": [], "cancelled": []}
    last = active_sessions(connection)[0]
    assert client.post(f"/api/network/sessions/{last['id']}/finish", json={"actor": "tests", "reason": "全部完成", "result": "completed"}).status_code == 200
    assert held_downlink(connection) == 0.0
    assert active_sessions(connection) == []
    total = connection.execute("SELECT COUNT(*) FROM acceleration_sessions").fetchone()[0]
    assert total == 4
    distinct = connection.execute("SELECT COUNT(DISTINCT incident_id) FROM acceleration_sessions").fetchone()[0]
    assert distinct == 4
    waiting = client.get("/api/network/waitlist").json()["items"]
    assert waiting == []


def test_session_expiry_triggers_deterministic_promotion(client):
    prepare_api(client, capacity_mbps=16)
    connection = get_connection()
    clock = FrozenClock(START)
    accel = NetworkAccelerationService(connection, clock)
    waitlist = WaitlistService(connection, clock)
    first = "subscriber-000000000101"
    second = "subscriber-000000000102"
    add_entitlement(accel, first)
    add_entitlement(accel, second)
    first_incident = make_incident(accel, sample_key="sample-expiry-1", subscriber=first)
    second_incident = make_incident(accel, sample_key="sample-expiry-2", subscriber=second)
    first_entry = waitlist.enqueue(first_incident, "tests")
    assert first_entry["state"] == "promoted"
    second_entry = waitlist.enqueue(second_incident, "tests")
    assert second_entry["state"] == "waiting"
    clock.advance(seconds=181)
    result = accel.expire_sessions("tests")
    assert result["expired"] == [first_entry["promoted_session_id"]]
    detail = waitlist.entry_detail(second_entry["id"])
    assert detail["state"] == "promoted"
    session = accel.get_session(detail["promoted_session_id"])
    assert session["started_at"] == to_storage(clock.now())
    assert session["expires_at"] == to_storage(datetime(2026, 9, 27, 8, 6, 1, tzinfo=UTC))
    assert session["events"][0]["detail"]["via"] == "waitlist"
    assert session["events"][0]["detail"]["trigger"] == "session_expired"
    assert held_downlink(connection) == 16.0
    incident_state = connection.execute("SELECT state FROM quality_incidents WHERE id=?", (second_incident,)).fetchone()["state"]
    assert incident_state == "accelerating"


def test_maintenance_completion_triggers_promotion(client):
    prepare_api(client, capacity_mbps=16)
    connection = get_connection()
    clock = FrozenClock(START)
    accel = NetworkAccelerationService(connection, clock)
    waitlist = WaitlistService(connection, clock)
    operations = NetworkOperationsService(connection, clock)
    subscriber = "subscriber-000000000201"
    add_entitlement(accel, subscriber)
    incident = make_incident(accel, sample_key="sample-maintenance-1", subscriber=subscriber)
    operations.create_maintenance(
        {
            "scenario_code": "metro-line-1",
            "segment_code": "rush-segment",
            "code": "rush-hour-fix",
            "reason": "早高峰射频调整",
            "starts_at": to_storage(START),
            "ends_at": to_storage(datetime(2026, 9, 27, 8, 30, tzinfo=UTC)),
            "drain_mode": "block_new",
            "actor": "operator",
        }
    )
    entry = waitlist.enqueue(incident, "tests")
    assert entry["state"] == "waiting"
    assert active_sessions(connection) == []
    clock.advance(minutes=30)
    outcome = NetworkOperationsService(connection, clock).activate_due_maintenance("scheduler")
    assert outcome["completed"] != []
    detail = waitlist.entry_detail(entry["id"])
    assert detail["state"] == "promoted"
    promoted_events = [event for event in detail["events"] if event["event_type"] == "promoted"]
    assert promoted_events[0]["detail"]["trigger"] == "maintenance_completed"
    assert len(active_sessions(connection)) == 1


def test_low_priority_gains_opportunity_as_it_waits(client):
    prepare_api(client, capacity_mbps=16)
    connection = get_connection()
    clock = FrozenClock(START)
    accel = NetworkAccelerationService(connection, clock)
    waitlist = WaitlistService(connection, clock)
    blocker = "subscriber-000000000301"
    low = "subscriber-000000000302"
    high_one = "subscriber-000000000303"
    high_two = "subscriber-000000000304"
    add_entitlement(accel, blocker)
    add_entitlement(accel, low)
    add_entitlement(accel, high_one, product="metro-premium")
    add_entitlement(accel, high_two, product="metro-premium")
    blocker_incident = make_incident(accel, sample_key="sample-aging-0", subscriber=blocker)
    low_incident = make_incident(
        accel,
        sample_key="sample-aging-low",
        subscriber=low,
        app_code="office-sync",
        latency_ms=330,
        packet_loss=0.05,
        downlink_mbps=4,
        uplink_mbps=2,
    )
    blocker_entry = waitlist.enqueue(blocker_incident, "tests")
    assert blocker_entry["state"] == "promoted"
    low_entry = waitlist.enqueue(low_incident, "tests")
    assert low_entry["state"] == "waiting"
    # 30 分钟后高优先级视频通话到达：静态分 420 仍高于低优先级 130+150。
    clock.advance(minutes=30)
    high_one_incident = make_incident(accel, sample_key="sample-aging-high-1", subscriber=high_one)
    high_one_entry = waitlist.enqueue(high_one_incident, "tests")
    items = waitlist.list_entries("metro-line-1")
    assert [item["id"] for item in items] == [high_one_entry["id"], low_entry["id"]]
    assert items[1]["factors"]["wait_points"] == 150.0
    accel.finish_session(blocker_entry["promoted_session_id"], "tests", "首次释放", "completed")
    assert waitlist.entry_detail(high_one_entry["id"])["state"] == "promoted"
    assert waitlist.entry_detail(low_entry["id"])["state"] == "waiting"
    # 又过 90 分钟：低优先级等待 120 分钟获得 600 等待分，超过新到高优先级的静态优势。
    clock.advance(minutes=90)
    high_two_incident = make_incident(accel, sample_key="sample-aging-high-2", subscriber=high_two)
    high_two_entry = waitlist.enqueue(high_two_incident, "tests")
    items = waitlist.list_entries("metro-line-1")
    assert [item["id"] for item in items] == [low_entry["id"], high_two_entry["id"]]
    low_factors = items[0]["factors"]
    assert low_factors["waiting_seconds"] == 7200
    assert low_factors["wait_points"] == 600.0
    assert low_factors["score"] == 730.0
    assert items[1]["factors"]["score"] == 420.0
    high_one_session = waitlist.entry_detail(high_one_entry["id"])["promoted_session_id"]
    accel.finish_session(high_one_session, "tests", "第二次释放", "completed")
    low_session = waitlist.entry_detail(low_entry["id"])["promoted_session_id"]
    assert low_session is not None
    assert waitlist.entry_detail(high_two_entry["id"])["state"] == "waiting"
    accel.finish_session(low_session, "tests", "第三次释放", "completed")
    assert waitlist.entry_detail(high_two_entry["id"])["state"] == "promoted"
    order = connection.execute(
        "SELECT w.id FROM waitlist_entries w JOIN acceleration_sessions s ON s.id=w.promoted_session_id ORDER BY s.id"
    ).fetchall()
    assert [row["id"] for row in order] == [blocker_entry["id"], high_one_entry["id"], low_entry["id"], high_two_entry["id"]]
    assert held_downlink(connection) == 16.0


def test_per_user_concurrency_limit_defers_busy_subscriber(client):
    prepare_api(client, capacity_mbps=32)
    connection = get_connection()
    clock = FrozenClock(START)
    accel = NetworkAccelerationService(connection, clock)
    waitlist = WaitlistService(connection, clock)
    sub_a = "subscriber-000000000401"
    sub_b = "subscriber-000000000402"
    sub_c = "subscriber-000000000403"
    for subscriber in (sub_a, sub_b, sub_c):
        add_entitlement(accel, subscriber)
    a_first = waitlist.enqueue(make_incident(accel, sample_key="sample-limit-a1", subscriber=sub_a), "tests")
    b_first = waitlist.enqueue(make_incident(accel, sample_key="sample-limit-b1", subscriber=sub_b), "tests")
    assert a_first["state"] == "promoted"
    assert b_first["state"] == "promoted"
    a_second = waitlist.enqueue(make_incident(accel, sample_key="sample-limit-a2", subscriber=sub_a), "tests")
    c_first = waitlist.enqueue(make_incident(accel, sample_key="sample-limit-c1", subscriber=sub_c), "tests")
    assert a_second["state"] == "waiting"
    assert c_first["state"] == "waiting"
    accel.finish_session(b_first["promoted_session_id"], "tests", "释放一个位置", "completed")
    # subA 已有一个进行中的会话，达到每用户并发上限，被跳过；subC 晋级。
    a_detail = waitlist.entry_detail(a_second["id"])
    assert a_detail["state"] == "waiting"
    assert a_detail["factors"]["active_sessions"] == 1
    assert a_detail["factors"]["eligible"] is False
    assert waitlist.entry_detail(c_first["id"])["state"] == "promoted"
    accel.finish_session(a_first["promoted_session_id"], "tests", "释放用户A的会话", "completed")
    assert waitlist.entry_detail(a_second["id"])["state"] == "promoted"
    assert held_downlink(connection) == 32.0


def test_waitlist_order_survives_restart(client):
    prepare_api(client, capacity_mbps=16)
    connection = get_connection()
    clock = FrozenClock(START)
    accel = NetworkAccelerationService(connection, clock)
    waitlist = WaitlistService(connection, clock)
    blocker = "subscriber-000000000501"
    add_entitlement(accel, blocker)
    blocker_entry = waitlist.enqueue(make_incident(accel, sample_key="sample-restart-0", subscriber=blocker), "tests")
    assert blocker_entry["state"] == "promoted"
    waiting_ids = []
    for index, product in enumerate(("metro-premium", "metro-basic", "metro-premium"), start=1):
        subscriber = f"subscriber-{500 + index:018d}"
        add_entitlement(accel, subscriber, product=product)
        incident = make_incident(accel, sample_key=f"sample-restart-{index}", subscriber=subscriber)
        entry = waitlist.enqueue(incident, "tests")
        assert entry["state"] == "waiting"
        waiting_ids.append(entry["id"])
    before = [(item["id"], item["rank"], item["factors"]["score"], item["requested_at"]) for item in waitlist.list_entries("metro-line-1")]
    close_connection()
    restarted_connection = get_connection()
    restarted = WaitlistService(restarted_connection, clock)
    after = [(item["id"], item["rank"], item["factors"]["score"], item["requested_at"]) for item in restarted.list_entries("metro-line-1")]
    assert after == before
    assert [item[0] for item in after] == [waiting_ids[0], waiting_ids[2], waiting_ids[1]]
    restarted_accel = NetworkAccelerationService(restarted_connection, clock)
    restarted_accel.finish_session(blocker_entry["promoted_session_id"], "tests", "重启后释放", "completed")
    assert restarted.entry_detail(waiting_ids[0])["state"] == "promoted"
    assert restarted.entry_detail(waiting_ids[2])["state"] == "waiting"


def test_lapsed_entitlement_expires_entry_instead_of_promoting(client):
    prepare_api(client, capacity_mbps=16)
    connection = get_connection()
    clock = FrozenClock(START)
    accel = NetworkAccelerationService(connection, clock)
    waitlist = WaitlistService(connection, clock)
    blocker = "subscriber-000000000601"
    waiter = "subscriber-000000000602"
    add_entitlement(accel, blocker)
    add_entitlement(accel, waiter, valid_until="2026-09-27T09:00:00Z")
    blocker_entry = waitlist.enqueue(make_incident(accel, sample_key="sample-lapse-0", subscriber=blocker), "tests")
    waiter_entry = waitlist.enqueue(make_incident(accel, sample_key="sample-lapse-1", subscriber=waiter), "tests")
    assert waiter_entry["state"] == "waiting"
    clock.advance(minutes=61)
    accel.finish_session(blocker_entry["promoted_session_id"], "tests", "权益已过期", "completed")
    detail = waitlist.entry_detail(waiter_entry["id"])
    assert detail["state"] == "expired"
    assert detail["decision_reason"] == "entitlement_inactive"
    assert active_sessions(connection) == []
    assert held_downlink(connection) == 0.0


def test_cancel_and_requeue_keeps_single_slot(client):
    prepare_api(client, capacity_mbps=16)
    connection = get_connection()
    clock = FrozenClock(START)
    accel = NetworkAccelerationService(connection, clock)
    waitlist = WaitlistService(connection, clock)
    blocker = "subscriber-000000000701"
    waiter = "subscriber-000000000702"
    add_entitlement(accel, blocker)
    add_entitlement(accel, waiter)
    blocker_entry = waitlist.enqueue(make_incident(accel, sample_key="sample-cancel-0", subscriber=blocker), "tests")
    incident = make_incident(accel, sample_key="sample-cancel-1", subscriber=waiter)
    entry = waitlist.enqueue(incident, "tests")
    assert entry["state"] == "waiting"
    clock.advance(minutes=10)
    cancelled = waitlist.cancel(entry["id"], "tests", "用户取消候补")
    assert cancelled["state"] == "cancelled"
    with pytest.raises(ConflictError):
        waitlist.cancel(entry["id"], "tests", "重复取消")
    clock.advance(minutes=10)
    requeued = waitlist.enqueue(incident, "tests")
    assert requeued["id"] == entry["id"]
    assert requeued["state"] == "waiting"
    assert requeued["requested_at"] == to_storage(clock.now())
    assert [event["event_type"] for event in requeued["events"]] == ["enqueued", "cancelled", "requeued"]
    count = connection.execute("SELECT COUNT(*) FROM waitlist_entries WHERE incident_id=?", (incident,)).fetchone()[0]
    assert count == 1
    accel.finish_session(blocker_entry["promoted_session_id"], "tests", "释放容量", "completed")
    assert waitlist.entry_detail(entry["id"])["state"] == "promoted"
