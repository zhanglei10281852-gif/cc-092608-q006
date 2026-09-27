from __future__ import annotations

from fastapi import APIRouter, Query

from app.core.errors import NotFoundError, ValidationError
from app.network.waitlist import WaitlistService
from app.network.waitlist_schemas import ProductTierUpsert, WaitlistCancel, WaitlistJoin, WaitlistPromote

router = APIRouter(prefix="/api/network", tags=["容量候补队列"])


def service() -> WaitlistService:
    return WaitlistService()


@router.post("/incidents/{incident_id}/waitlist", status_code=201)
def join_waitlist(incident_id: int, payload: WaitlistJoin):
    return service().enqueue(incident_id, payload.actor)


@router.get("/waitlist")
def list_waitlist(scenario_code: str | None = None, state: str = Query(default="waiting")):
    if state not in {"waiting", "promoted", "cancelled", "expired"}:
        raise ValidationError("候补状态必须是 waiting、promoted、cancelled 或 expired")
    return {"items": service().list_entries(scenario_code, state)}


@router.get("/waitlist/{entry_id}")
def waitlist_detail(entry_id: int):
    return service().entry_detail(entry_id)


@router.post("/waitlist/{entry_id}/cancel")
def cancel_waitlist(entry_id: int, payload: WaitlistCancel):
    return service().cancel(entry_id, payload.actor, payload.reason)


@router.post("/waitlist/promote")
def promote_waitlist(payload: WaitlistPromote):
    waitlist = service()
    scenario_id = None
    if payload.scenario_code:
        scenario = waitlist.repository.scenario_by_code(payload.scenario_code)
        if scenario is None:
            raise NotFoundError("网络场景不存在")
        scenario_id = int(scenario["id"])
    return waitlist.promote_waiting(actor=payload.actor, scenario_id=scenario_id, trigger="manual")


@router.post("/product-tiers", status_code=201)
def upsert_product_tier(payload: ProductTierUpsert):
    return service().upsert_product_tier(payload.model_dump())


@router.get("/product-tiers")
def list_product_tiers():
    return {"items": service().list_product_tiers()}
