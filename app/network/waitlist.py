from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Mapping

from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, NotFoundError
from app.database import get_connection, transaction
from app.network.repository import NetworkRepository
from app.network.rules import allocation_for
from app.network.schema import ensure_network_schema

# 候补排序权重：严重度基点 + 应用优先级 + 权益等级 * 等级权重 + 等待分钟 * 等待速率。
# 所有因子随候补记录持久化，等待时长由注入时钟计算，同一逻辑时间下顺序确定且可解释。
SEVERITY_POINTS = {"minor": 100, "major": 200, "critical": 300}
TIER_WEIGHT = 10
WAIT_POINTS_PER_MINUTE = 5.0
DEFAULT_PER_USER_SESSION_LIMIT = 1


def waiting_seconds(entry: Mapping[str, Any], now: datetime) -> float:
    requested = from_storage(entry["requested_at"])
    if requested is None:
        return 0.0
    return max(0.0, (now - requested).total_seconds())


def score_breakdown(entry: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    waited = waiting_seconds(entry, now)
    severity_points = SEVERITY_POINTS[entry["severity"]]
    app_priority = int(entry["app_priority"])
    tier_points = int(entry["entitlement_tier"]) * TIER_WEIGHT
    wait_points = round(waited / 60.0 * WAIT_POINTS_PER_MINUTE, 6)
    score = round(severity_points + app_priority + tier_points + wait_points, 6)
    return {
        "severity": entry["severity"],
        "severity_points": severity_points,
        "app_priority": app_priority,
        "app_priority_points": app_priority,
        "entitlement_tier": int(entry["entitlement_tier"]),
        "tier_weight": TIER_WEIGHT,
        "tier_points": tier_points,
        "waiting_seconds": int(waited),
        "wait_points_per_minute": WAIT_POINTS_PER_MINUTE,
        "wait_points": wait_points,
        "score": score,
    }


def rank_key(entry: Mapping[str, Any], now: datetime) -> tuple[float, str, int]:
    return (-score_breakdown(entry, now)["score"], entry["requested_at"], int(entry["id"]))


class WaitlistService:
    def __init__(
        self,
        connection: sqlite3.Connection | None = None,
        clock: Clock | None = None,
        per_user_session_limit: int = DEFAULT_PER_USER_SESSION_LIMIT,
    ) -> None:
        self.connection = connection or get_connection()
        ensure_network_schema(self.connection)
        self.clock = clock or SystemClock()
        self.repository = NetworkRepository(self.connection)
        self.per_user_session_limit = per_user_session_limit

    def upsert_product_tier(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            connection.execute(
                "INSERT INTO product_tiers(product_code,tier,created_by,created_at,updated_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(product_code) DO UPDATE SET tier=excluded.tier,updated_at=excluded.updated_at",
                (payload["product_code"], payload["tier"], payload["actor"], now, now),
            )
            return dict(NetworkRepository(connection).product_tier_by_code(payload["product_code"]))

    def list_product_tiers(self) -> list[dict[str, Any]]:
        return self.repository.list_product_tiers()

    def enqueue(self, incident_id: int, actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = NetworkRepository(connection)
            incident = repository.incident_by_id(incident_id)
            if incident is None:
                raise NotFoundError("质差事件不存在")
            existing = repository.waitlist_entry_by_incident(incident_id)
            if existing is not None and existing["state"] in {"waiting", "promoted"}:
                entry_id = int(existing["id"])
                scenario_id = int(existing["scenario_id"])
            else:
                snapshot = self._snapshot(repository, incident, now)
                detail = {
                    "severity": snapshot["severity"],
                    "app_priority": snapshot["app_priority"],
                    "entitlement_tier": snapshot["entitlement_tier"],
                }
                if existing is None:
                    cursor = connection.execute(
                        "INSERT INTO waitlist_entries(incident_id,subscriber_hash,app_id,scenario_id,segment_id,policy_version_id,"
                        "severity,app_priority,entitlement_tier,allocated_downlink_mbps,allocated_uplink_mbps,priority,duration_seconds,requested_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            incident_id,
                            snapshot["subscriber_hash"],
                            snapshot["app_id"],
                            snapshot["scenario_id"],
                            snapshot["segment_id"],
                            snapshot["policy_version_id"],
                            snapshot["severity"],
                            snapshot["app_priority"],
                            snapshot["entitlement_tier"],
                            snapshot["allocated_downlink_mbps"],
                            snapshot["allocated_uplink_mbps"],
                            snapshot["priority"],
                            snapshot["duration_seconds"],
                            now,
                        ),
                    )
                    entry_id = int(cursor.lastrowid)
                    self._event(connection, entry_id, "enqueued", actor, detail, now)
                else:
                    entry_id = int(existing["id"])
                    connection.execute(
                        "UPDATE waitlist_entries SET subscriber_hash=?,app_id=?,scenario_id=?,segment_id=?,policy_version_id=?,"
                        "severity=?,app_priority=?,entitlement_tier=?,allocated_downlink_mbps=?,allocated_uplink_mbps=?,priority=?,"
                        "duration_seconds=?,state='waiting',requested_at=?,decided_at=NULL,promoted_session_id=NULL,decision_reason='',"
                        "version=version+1 WHERE id=?",
                        (
                            snapshot["subscriber_hash"],
                            snapshot["app_id"],
                            snapshot["scenario_id"],
                            snapshot["segment_id"],
                            snapshot["policy_version_id"],
                            snapshot["severity"],
                            snapshot["app_priority"],
                            snapshot["entitlement_tier"],
                            snapshot["allocated_downlink_mbps"],
                            snapshot["allocated_uplink_mbps"],
                            snapshot["priority"],
                            snapshot["duration_seconds"],
                            now,
                            entry_id,
                        ),
                    )
                    self._event(connection, entry_id, "requeued", actor, detail, now)
                scenario_id = int(snapshot["scenario_id"])
        self.promote_waiting(actor=actor, scenario_id=scenario_id, trigger="enqueued")
        return self.entry_detail(entry_id)

    def cancel(self, entry_id: int, actor: str, reason: str) -> dict[str, Any]:
        entry = self.repository.waitlist_entry_by_id(entry_id)
        if entry is None:
            raise NotFoundError("候补记录不存在")
        if entry["state"] != "waiting":
            raise ConflictError("只有等待中的候补可以取消")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            connection.execute(
                "UPDATE waitlist_entries SET state='cancelled',decided_at=?,decision_reason=?,version=version+1 WHERE id=? AND state='waiting'",
                (now, reason, entry_id),
            )
            self._event(connection, entry_id, "cancelled", actor, {"reason": reason, "trigger": "manual"}, now)
        return self.entry_detail(entry_id)

    def entry_detail(self, entry_id: int, connection: sqlite3.Connection | None = None) -> dict[str, Any]:
        connection = connection or self.connection
        row = connection.execute(
            "SELECT w.*,n.code AS scenario_code,g.code AS segment_code,a.app_code FROM waitlist_entries w "
            "JOIN network_scenarios n ON n.id=w.scenario_id LEFT JOIN network_segments g ON g.id=w.segment_id "
            "JOIN application_profiles a ON a.id=w.app_id WHERE w.id=?",
            (entry_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError("候补记录不存在")
        result = dict(row)
        if result["state"] == "waiting":
            rank, factors = self._position(connection, result, self.clock.now())
            result["rank"] = rank
            result["factors"] = factors
        else:
            result["rank"] = None
            result["factors"] = None
        result["events"] = self._events(connection, entry_id)
        return result

    def list_entries(self, scenario_code: str | None = None, state: str = "waiting") -> list[dict[str, Any]]:
        scenario_id = None
        if scenario_code:
            scenario = self.repository.scenario_by_code(scenario_code)
            if scenario is None:
                raise NotFoundError("网络场景不存在")
            scenario_id = int(scenario["id"])
        clauses = ["w.state=?"]
        params: list[Any] = [state]
        if scenario_id is not None:
            clauses.append("w.scenario_id=?")
            params.append(scenario_id)
        rows = self.connection.execute(
            "SELECT w.*,n.code AS scenario_code,g.code AS segment_code,a.app_code FROM waitlist_entries w "
            "JOIN network_scenarios n ON n.id=w.scenario_id LEFT JOIN network_segments g ON g.id=w.segment_id "
            "JOIN application_profiles a ON a.id=w.app_id WHERE " + " AND ".join(clauses) + " ORDER BY w.id",
            params,
        ).fetchall()
        items = [dict(row) for row in rows]
        if state != "waiting":
            for item in items:
                item["rank"] = None
                item["factors"] = None
            return items
        now = self.clock.now()
        buckets: dict[tuple[int, Any], list[dict[str, Any]]] = {}
        for item in items:
            buckets.setdefault((item["scenario_id"], item["segment_id"]), []).append(item)
        decorated: list[dict[str, Any]] = []
        for key in sorted(buckets, key=lambda bucket: (bucket[0], bucket[1] is not None, bucket[1] or 0)):
            ranked = sorted(buckets[key], key=lambda entry: rank_key(entry, now))
            for position, entry in enumerate(ranked, start=1):
                active = self.repository.active_session_count(entry["subscriber_hash"], entry["scenario_id"])
                entry["rank"] = position
                entry["factors"] = self._factors(entry, now, active)
                decorated.append(entry)
        decorated.sort(key=lambda entry: (entry["scenario_code"], entry["segment_code"] or "", entry["rank"]))
        return decorated

    def promote_waiting(
        self,
        *,
        actor: str = "waitlist-scheduler",
        scenario_id: int | None = None,
        trigger: str = "manual",
    ) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        outcome: dict[str, Any] = {"promoted": [], "expired": [], "cancelled": []}
        with transaction(immediate=True) as connection:
            repository = NetworkRepository(connection)
            buckets: dict[tuple[int, Any], list[sqlite3.Row]] = {}
            for entry in repository.waiting_entries(scenario_id):
                buckets.setdefault((int(entry["scenario_id"]), entry["segment_id"]), []).append(entry)
            for scenario_key, segment_key in sorted(buckets, key=lambda bucket: (bucket[0], bucket[1] is not None, bucket[1] or 0)):
                scenario = repository.scenario_by_id(scenario_key)
                if scenario is None:
                    continue
                segment = repository.segment_by_id(segment_key) if segment_key is not None else None
                limit = float(segment["capacity_mbps"] if segment else scenario["capacity_mbps"])
                if self._maintenance_block(connection, scenario_key, segment_key, now) is not None:
                    continue
                ranked = sorted(buckets[(scenario_key, segment_key)], key=lambda entry: rank_key(dict(entry), now_value))
                for entry in ranked:
                    session = repository.session_by_incident(entry["incident_id"])
                    if session is not None:
                        self._decide(connection, entry, "promoted", now, actor, "session_exists", trigger, session_id=int(session["id"]))
                        outcome["promoted"].append({"entry_id": int(entry["id"]), "session_id": int(session["id"])})
                        continue
                    incident = repository.incident_by_id(entry["incident_id"])
                    if incident is None or incident["state"] != "open":
                        self._decide(connection, entry, "cancelled", now, actor, "incident_not_open", trigger)
                        outcome["cancelled"].append(int(entry["id"]))
                        continue
                    entitlement = repository.active_entitlement(entry["subscriber_hash"], scenario_key, now)
                    if entitlement is None:
                        self._decide(connection, entry, "expired", now, actor, "entitlement_inactive", trigger)
                        outcome["expired"].append(int(entry["id"]))
                        continue
                    used = repository.active_capacity(scenario_key, segment_key)
                    if used["sessions"] >= int(scenario["max_concurrent_sessions"]):
                        break
                    if repository.active_session_count(entry["subscriber_hash"], scenario_key) >= self.per_user_session_limit:
                        continue
                    if used["downlink_mbps"] + float(entry["allocated_downlink_mbps"]) > limit:
                        continue
                    session_id = self._start_session(connection, entry, now_value, now, actor, trigger)
                    outcome["promoted"].append({"entry_id": int(entry["id"]), "session_id": session_id})
        return outcome

    def _snapshot(self, repository: NetworkRepository, incident: sqlite3.Row, now: str) -> dict[str, Any]:
        if incident["state"] != "open":
            raise ConflictError("只有待处理事件可以候补加速")
        if repository.session_by_incident(incident["id"]) is not None:
            raise ConflictError("事件已存在加速会话")
        sample = repository.sample_by_id(incident["sample_id"])
        app = repository.application_by_id(incident["app_id"])
        entitlement = repository.active_entitlement(sample["subscriber_hash"], incident["scenario_id"], now)
        if entitlement is None:
            raise ConflictError("用户没有当前场景的有效加速权益")
        policy = repository.effective_policy(incident["scenario_id"], now)
        if policy is None:
            raise ConflictError("场景没有已生效的加速策略")
        allocation = allocation_for(dict(app), incident["severity"], json.loads(policy["rules_json"]))
        tier_row = repository.product_tier_by_code(entitlement["product_code"])
        return {
            "subscriber_hash": sample["subscriber_hash"],
            "app_id": int(incident["app_id"]),
            "scenario_id": int(incident["scenario_id"]),
            "segment_id": incident["segment_id"],
            "policy_version_id": int(policy["id"]),
            "severity": incident["severity"],
            "app_priority": int(app["default_priority"]),
            "entitlement_tier": int(tier_row["tier"]) if tier_row else 0,
            "allocated_downlink_mbps": allocation.downlink_mbps,
            "allocated_uplink_mbps": allocation.uplink_mbps,
            "priority": allocation.priority,
            "duration_seconds": allocation.duration_seconds,
        }

    def _start_session(
        self,
        connection: sqlite3.Connection,
        entry: sqlite3.Row,
        now_value: datetime,
        now: str,
        actor: str,
        trigger: str,
    ) -> int:
        expires = to_storage(now_value + timedelta(seconds=int(entry["duration_seconds"])))
        cursor = connection.execute(
            "INSERT INTO acceleration_sessions(incident_id,subscriber_hash,app_id,scenario_id,segment_id,policy_version_id,"
            "allocated_downlink_mbps,allocated_uplink_mbps,priority,started_at,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                entry["incident_id"],
                entry["subscriber_hash"],
                entry["app_id"],
                entry["scenario_id"],
                entry["segment_id"],
                entry["policy_version_id"],
                entry["allocated_downlink_mbps"],
                entry["allocated_uplink_mbps"],
                entry["priority"],
                now,
                expires,
            ),
        )
        session_id = int(cursor.lastrowid)
        connection.execute(
            "INSERT INTO capacity_reservations(session_id,scenario_id,segment_id,downlink_mbps,uplink_mbps,held_at) VALUES(?,?,?,?,?,?)",
            (session_id, entry["scenario_id"], entry["segment_id"], entry["allocated_downlink_mbps"], entry["allocated_uplink_mbps"], now),
        )
        connection.execute("UPDATE quality_incidents SET state='accelerating',version=version+1 WHERE id=?", (entry["incident_id"],))
        policy = connection.execute("SELECT version_no FROM policy_versions WHERE id=?", (entry["policy_version_id"],)).fetchone()
        connection.execute(
            "INSERT INTO session_events(session_id,event_type,actor,detail_json,created_at) VALUES(?,?,?,?,?)",
            (
                session_id,
                "started",
                actor,
                json.dumps(
                    {"via": "waitlist", "waitlist_entry_id": int(entry["id"]), "trigger": trigger, "policy_version": policy["version_no"] if policy else None},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                now,
            ),
        )
        self._decide(connection, entry, "promoted", now, actor, "capacity_available", trigger, session_id=session_id)
        return session_id

    def _position(
        self,
        connection: sqlite3.Connection,
        entry: dict[str, Any],
        now: datetime,
    ) -> tuple[int | None, dict[str, Any] | None]:
        rows = connection.execute(
            "SELECT * FROM waitlist_entries WHERE state='waiting' AND scenario_id=? AND segment_id IS ?",
            (entry["scenario_id"], entry["segment_id"]),
        ).fetchall()
        ranked = sorted((dict(row) for row in rows), key=lambda candidate: rank_key(candidate, now))
        for position, candidate in enumerate(ranked, start=1):
            if candidate["id"] == entry["id"]:
                active = NetworkRepository(connection).active_session_count(entry["subscriber_hash"], entry["scenario_id"])
                return position, self._factors(entry, now, active)
        return None, None

    def _factors(self, entry: Mapping[str, Any], now: datetime, active_sessions: int) -> dict[str, Any]:
        factors = score_breakdown(entry, now)
        factors["active_sessions"] = active_sessions
        factors["per_user_session_limit"] = self.per_user_session_limit
        factors["eligible"] = active_sessions < self.per_user_session_limit
        return factors

    def _maintenance_block(self, connection: sqlite3.Connection, scenario_id: int, segment_id: int | None, now: str) -> dict[str, Any] | None:
        from app.network.operations import NetworkOperationsService

        return NetworkOperationsService(connection, self.clock).blocks_new_session(scenario_id, segment_id, now)

    def _decide(
        self,
        connection: sqlite3.Connection,
        entry: sqlite3.Row,
        state: str,
        now: str,
        actor: str,
        reason: str,
        trigger: str,
        *,
        session_id: int | None = None,
    ) -> None:
        connection.execute(
            "UPDATE waitlist_entries SET state=?,decided_at=?,promoted_session_id=?,decision_reason=?,version=version+1 WHERE id=?",
            (state, now, session_id, reason, entry["id"]),
        )
        self._event(connection, int(entry["id"]), state, actor, {"reason": reason, "trigger": trigger, "session_id": session_id}, now)

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        entry_id: int,
        event_type: str,
        actor: str,
        detail: dict[str, Any],
        now: str,
    ) -> None:
        connection.execute(
            "INSERT INTO operation_events(resource_type,resource_id,event_type,actor,detail_json,created_at) VALUES('waitlist',?,?,?,?,?)",
            (entry_id, event_type, actor, json.dumps(detail, ensure_ascii=False, sort_keys=True), now),
        )

    @staticmethod
    def _events(connection: sqlite3.Connection, entry_id: int) -> list[dict[str, Any]]:
        rows = connection.execute(
            "SELECT * FROM operation_events WHERE resource_type='waitlist' AND resource_id=? ORDER BY id",
            (entry_id,),
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item.pop("detail_json"))
            result.append(item)
        return result
