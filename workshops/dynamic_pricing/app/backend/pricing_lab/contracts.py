"""Validated boundaries: business facts, immutable evidence, and mock receipts."""
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

PositiveInt = Annotated[int, Field(strict=True, gt=0)]
NonnegativeInt = Annotated[int, Field(strict=True, ge=0)]
Identifier = Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")]
Age = Annotated[float, Field(ge=0, allow_inf_nan=False)]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    @field_validator("*", mode="after")
    @classmethod
    def utc_dates(cls, value):
        return value.astimezone(UTC) if isinstance(value, datetime) else value


class Scope(Contract):
    owner_id: Identifier
    scenario_id: Identifier
    generation: PositiveInt

    @property
    def sandbox_id(self) -> str:
        return f"{self.owner_id}/{self.scenario_id}"


class Subject(Contract):
    sku: PositiveInt
    store_id: PositiveInt
    channel: Literal["store"] = "store"


class Snapshot(Contract):
    snapshot_id: UUID
    scope: Scope
    subject: Subject
    scenario_cutoff: AwareDatetime
    captured_at: AwareDatetime
    source_versions: dict[str, str]
    source_ages_seconds: dict[str, Age]
    stock_units: NonnegativeInt
    inventory_expires_at: AwareDatetime
    expires_at: AwareDatetime
    current_price_minor: PositiveInt
    reference_price_minor: PositiveInt
    unit_cost_minor: NonnegativeInt
    selling_cost_minor: NonnegativeInt
    disposal_cost_minor: NonnegativeInt
    rule_version: PositiveInt
    applied_price_version: PositiveInt


class Candidate(Contract):
    candidate_id: UUID
    snapshot: Snapshot
    model_version: str
    price_minor: PositiveInt
    created_at: AwareDatetime
    expires_at: AwareDatetime
    projection: dict[str, Decimal]


class PolicyResult(Contract):
    status: Literal["eligible", "requires_review", "rejected", "held"]
    reason_ids: list[str]


class SaleRequest(Contract):
    event_id: UUID
    subject: Subject
    quantity: PositiveInt
    occurred_at: AwareDatetime
    expected_price_version: PositiveInt


class Receipt(Contract):
    receipt_id: UUID
    decision_id: UUID
    scope: Scope
    price_minor: PositiveInt
    applied_price_version: PositiveInt
    acknowledged_at: AwareDatetime


class WorkshopError(ValueError):
    """Safe, structured error. Messages never contain credentials or raw SQL."""

    def __init__(self, code: str, action: str):
        self.code, self.action = code, action
        super().__init__(f"{code}: {action}")


class StaleGeneration(WorkshopError):
    def __init__(self):
        super().__init__("generation_changed", "Restore the current checkpoint and create a fresh proposal.")
