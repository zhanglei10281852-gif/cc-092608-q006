from __future__ import annotations

from pydantic import BaseModel, Field


class WaitlistJoin(BaseModel):
    actor: str = Field(min_length=1, max_length=120)


class WaitlistCancel(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=500)


class WaitlistPromote(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    scenario_code: str | None = Field(default=None, max_length=64)


class ProductTierUpsert(BaseModel):
    product_code: str = Field(min_length=2, max_length=80)
    tier: int = Field(ge=0, le=9)
    actor: str = Field(min_length=1, max_length=120)
