"""持久化候补队列：可解释排序与确定性晋级。

排序总分由五个可解释因素组成，全部因素持久化在候补记录上或可由库内状态推导，
服务重启后同一固定时钟下顺序不变：

- 事件严重度：critical=3000 / major=2000 / minor=1000
- 应用优先级：应用画像 default_priority × 10（0..1000）
- 权益等级：权益 tier_level × 5（0..500）
- 等待时长：每等待 1 分钟 +20 分，封顶 4500 分（等于静态因素最大分差，
  保证任何候补最终都能超过新到的高优先级请求，避免饿死）
- 每用户并发惩罚：该用户当前每个活跃会话 -300 分，并作为晋级硬上限

总分相同按 requested_at、id 升序，保证完全确定。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Mapping

from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import NotFoundError
from app.database import get_connection
from app.network.repository import NetworkRepository
from app.network.schema import ensure_network_schema

SEVERITY_SCORE = {"critical": 3000, "major": 2000, "minor": 1000}
APP_PRIORITY_WEIGHT = 10
TIER_WEIGHT = 5
AGING_POINTS_PER_MINUTE = 20
AGING_MAX_POINTS = 4500
CONCURRENCY_PENALTY_PER_SESSION = 300
DEFAULT_MAX_ACTIVE_PER_SUBSCRIBER = 1


def ranking_factors(entry: Mapping[str, Any], active_sessions: int, now: datetime) -> dict[str, Any]:
    requested = from_storage(entry["requested_at"])
    waited_seconds = max(0, int((now - requested).total_seconds()))
    aging_score = min(waited_seconds // 60 * AGING_POINTS_PER_MINUTE, AGING_MAX_POINTS)
    severity_score = SEVERITY_SCORE[entry["severity"]]
    app_priority_score = int(entry["app_priority"]) * APP_PRIORITY_WEIGHT
    tier_score = int(entry["tier_level"]) * TIER_WEIGHT
    concurrency_penalty = active_sessions * CONCURRENCY_PENALTY_PER_SESSION
    total = severity_score + app_priority_score + tier_score + aging_score - concurrency_penalty
    return {
        "severity": entry["severity"],
        "severity_score": severity_score,
        "app_priority": int(entry["app_priority"]),
        "app_priority_score": app_priority_score,
        "tier_level": int(entry["tier_level"]),
        "tier_score": tier_score,
        "waited_seconds": waited_seconds,
        "aging_score": aging_score,
        "active_sessions": active_sessions,
        "concurrency_penalty": concurrency_penalty,
        "total_score": total,
    }


def _sort_key(item: tuple[sqlite3.Row, dict[str, Any]]) -> tuple[Any, ...]:
    row, factors = item
    return (-factors["total_score"], row["requested_at"], row["id"])


class WaitlistService:
    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        ensure_network_schema(self.connection)
        self.clock = clock or SystemClock()
        self.repository = NetworkRepository(self.connection)

    def enqueue(
        self,
        connection: sqlite3.Connection,
        entry: dict[str, Any],
        actor: str,
        now: datetime,
        event_detail: dict[str, Any] | None = None,
    ) -> tuple[int, bool]:
        """写入候补记录；同一事件已有等待中的记录时返回原记录（只占一个位置）。"""
        now_text = to_storage(now)
        try:
            cursor = connection.execute(
                "INSERT INTO acceleration_waitlist(incident_id,subscriber_hash,app_id,scenario_id,segment_id,policy_version_id,"
                "severity,app_priority,tier_level,session_priority,allocated_downlink_mbps,allocated_uplink_mbps,duration_seconds,"
                "max_active_per_subscriber,queue_reason,requested_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    entry["incident_id"], entry["subscriber_hash"], entry["app_id"], entry["scenario_id"], entry["segment_id"],
                    entry["policy_version_id"], entry["severity"], entry["app_priority"], entry["tier_level"], entry["session_priority"],
                    entry["allocated_downlink_mbps"], entry["allocated_uplink_mbps"], entry["duration_seconds"],
                    entry["max_active_per_subscriber"], entry["queue_reason"], now_text,
                ),
            )
            entry_id = int(cursor.lastrowid)
        except sqlite3.IntegrityError:
            existing = NetworkRepository(connection).waiting_entry_for_incident(entry["incident_id"])
            if existing is None:
                raise
            return int(existing["id"]), True
        repository = NetworkRepository(connection)
        stored = repository.waitlist_entry(entry_id)
        active = repository.subscriber_active_sessions(entry["subscriber_hash"], entry["scenario_id"])
        detail = {"reason": entry["queue_reason"], "factors": ranking_factors(stored, active, now)}
        detail.update(event_detail or {})
        self._event(connection, entry_id, "enqueued", actor, detail, now_text)
        return entry_id, False

    def promote_scope(
        self,
        connection: sqlite3.Connection,
        scenario_id: int,
        segment_id: int | None,
        actor: str,
        now: datetime,
    ) -> list[int]:
        """按确定顺序晋级候补：容量释放、维护结束或会话过期后在同一事务内调用。"""
        repository = NetworkRepository(connection)
        now_text = to_storage(now)
        if repository.blocking_maintenance(scenario_id, segment_id, now_text) is not None:
            return []
        scenario = repository.scenario_by_id(scenario_id)
        segment = repository.segment_by_id(segment_id) if segment_id is not None else None
        limit = float(segment["capacity_mbps"] if segment else scenario["capacity_mbps"])
        ordered = self._ordered_waiting(repository, scenario_id, segment_id, now)
        if not ordered:
            return []
        used = repository.active_capacity(scenario_id, segment_id)
        sessions_used = used["sessions"]
        downlink_used = used["downlink_mbps"]
        active_counts: dict[str, int] = {}
        promoted: list[int] = []
        for row, _factors in ordered:
            incident = repository.incident_by_id(row["incident_id"])
            if incident is None or incident["state"] != "open":
                self._cancel(connection, row, "incident_closed", actor, now_text)
                continue
            entitlement = repository.active_entitlement(row["subscriber_hash"], scenario_id, now_text)
            if entitlement is None:
                self._cancel(connection, row, "entitlement_lapsed", actor, now_text)
                continue
            subscriber = row["subscriber_hash"]
            if subscriber not in active_counts:
                active_counts[subscriber] = repository.subscriber_active_sessions(subscriber, scenario_id)
            if active_counts[subscriber] >= int(row["max_active_per_subscriber"]):
                continue
            if sessions_used >= int(scenario["max_concurrent_sessions"]):
                break
            if downlink_used + float(row["allocated_downlink_mbps"]) > limit:
                continue
            policy = repository.policy_by_id(row["policy_version_id"])
            if policy is None:
                continue
            session_id = self._allocate(connection, row, policy, actor, now, now_text)
            connection.execute(
                "UPDATE acceleration_waitlist SET state='promoted',decided_at=?,session_id=?,version=version+1 WHERE id=? AND state='waiting'",
                (now_text, session_id, row["id"]),
            )
            self._event(connection, row["id"], "promoted", actor, {"session_id": session_id}, now_text)
            sessions_used += 1
            downlink_used += float(row["allocated_downlink_mbps"])
            active_counts[subscriber] += 1
            promoted.append(int(row["id"]))
        return promoted

    def detail(self, entry_id: int, connection: sqlite3.Connection | None = None) -> dict[str, Any]:
        repository = NetworkRepository(connection or self.connection)
        row = repository.waitlist_entry(entry_id)
        if row is None:
            raise NotFoundError("候补记录不存在")
        now = self.clock.now()
        ranks = self._ranks(repository, row["scenario_id"], row["segment_id"], now)
        result = self._present(repository, row, ranks.get(int(row["id"])), now)
        result["events"] = repository.waitlist_events(entry_id)
        return result

    def list_entries(self, scenario_id: int | None = None, state: str | None = "waiting", limit: int = 100) -> list[dict[str, Any]]:
        rows = self.repository.waitlist_entries(scenario_id=scenario_id, state=state, limit=limit)
        now = self.clock.now()
        rank_maps: dict[tuple[int, Any], dict[int, int]] = {}
        for row in rows:
            key = (int(row["scenario_id"]), row["segment_id"])
            if key not in rank_maps:
                rank_maps[key] = self._ranks(self.repository, key[0], row["segment_id"], now)
        result = []
        for row in rows:
            key = (int(row["scenario_id"]), row["segment_id"])
            result.append(self._present(self.repository, row, rank_maps[key].get(int(row["id"])), now))
        scope_order = {(int(row["scenario_id"]), row["segment_id"]): index for index, row in enumerate(rows)}
        result.sort(key=lambda item: (scope_order[(item["scenario_id"], item["segment_id"])], item["rank"] if item["rank"] is not None else 1_000_000, item["requested_at"], item["id"]))
        return result

    def _allocate(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        policy: sqlite3.Row,
        actor: str,
        now: datetime,
        now_text: str,
    ) -> int:
        expires = to_storage(now + timedelta(seconds=int(row["duration_seconds"])))
        cursor = connection.execute(
            "INSERT INTO acceleration_sessions(incident_id,subscriber_hash,app_id,scenario_id,segment_id,policy_version_id,"
            "allocated_downlink_mbps,allocated_uplink_mbps,priority,started_at,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                row["incident_id"], row["subscriber_hash"], row["app_id"], row["scenario_id"], row["segment_id"],
                row["policy_version_id"], row["allocated_downlink_mbps"], row["allocated_uplink_mbps"],
                int(row["session_priority"]),
                now_text, expires,
            ),
        )
        session_id = int(cursor.lastrowid)
        connection.execute(
            "INSERT INTO capacity_reservations(session_id,scenario_id,segment_id,downlink_mbps,uplink_mbps,held_at) VALUES(?,?,?,?,?,?)",
            (session_id, row["scenario_id"], row["segment_id"], row["allocated_downlink_mbps"], row["allocated_uplink_mbps"], now_text),
        )
        connection.execute("UPDATE quality_incidents SET state='accelerating',version=version+1 WHERE id=?", (row["incident_id"],))
        connection.execute(
            "INSERT INTO session_events(session_id,event_type,actor,detail_json,created_at) VALUES(?,?,?,?,?)",
            (
                session_id, "started", actor,
                json.dumps({"trigger": "waitlist_promotion", "waitlist_entry_id": row["id"], "policy_version": policy["version_no"]}, ensure_ascii=False, sort_keys=True),
                now_text,
            ),
        )
        return session_id

    def _ordered_waiting(self, repository: NetworkRepository, scenario_id: int, segment_id: int | None, now: datetime) -> list[tuple[sqlite3.Row, dict[str, Any]]]:
        rows = repository.waiting_entries_for_scope(scenario_id, segment_id)
        counts: dict[str, int] = {}
        items = []
        for row in rows:
            subscriber = row["subscriber_hash"]
            if subscriber not in counts:
                counts[subscriber] = repository.subscriber_active_sessions(subscriber, scenario_id)
            items.append((row, ranking_factors(row, counts[subscriber], now)))
        items.sort(key=_sort_key)
        return items

    def _ranks(self, repository: NetworkRepository, scenario_id: int, segment_id: int | None, now: datetime) -> dict[int, int]:
        return {int(row["id"]): index for index, (row, _factors) in enumerate(self._ordered_waiting(repository, scenario_id, segment_id, now), start=1)}

    def _present(self, repository: NetworkRepository, row: sqlite3.Row, rank: int | None, now: datetime) -> dict[str, Any]:
        waiting = row["state"] == "waiting"
        factor_time = now if waiting else (from_storage(row["decided_at"]) or now)
        active = repository.subscriber_active_sessions(row["subscriber_hash"], row["scenario_id"])
        return {
            "id": int(row["id"]),
            "incident_id": int(row["incident_id"]),
            "session_id": row["session_id"],
            "state": row["state"],
            "queue_reason": row["queue_reason"],
            "cancel_reason": row["cancel_reason"],
            "scenario_id": int(row["scenario_id"]),
            "scenario_code": row["scenario_code"],
            "segment_id": row["segment_id"],
            "segment_code": row["segment_code"],
            "app_code": row["app_code"],
            "subscriber_hash": row["subscriber_hash"],
            "rank": rank if waiting else None,
            "factors": ranking_factors(row, active, factor_time),
            "allocation": {
                "downlink_mbps": float(row["allocated_downlink_mbps"]),
                "uplink_mbps": float(row["allocated_uplink_mbps"]),
                "duration_seconds": int(row["duration_seconds"]),
            },
            "max_active_per_subscriber": int(row["max_active_per_subscriber"]),
            "requested_at": row["requested_at"],
            "decided_at": row["decided_at"],
        }

    def _cancel(self, connection: sqlite3.Connection, row: sqlite3.Row, reason: str, actor: str, now_text: str) -> None:
        connection.execute(
            "UPDATE acceleration_waitlist SET state='cancelled',decided_at=?,cancel_reason=?,version=version+1 WHERE id=? AND state='waiting'",
            (now_text, reason, row["id"]),
        )
        self._event(connection, row["id"], "cancelled", actor, {"reason": reason}, now_text)

    @staticmethod
    def _event(connection: sqlite3.Connection, entry_id: int, event_type: str, actor: str, detail: dict[str, Any], now_text: str) -> None:
        connection.execute(
            "INSERT INTO waitlist_events(entry_id,event_type,actor,detail_json,created_at) VALUES(?,?,?,?,?)",
            (entry_id, event_type, actor, json.dumps(detail, ensure_ascii=False, sort_keys=True), now_text),
        )
