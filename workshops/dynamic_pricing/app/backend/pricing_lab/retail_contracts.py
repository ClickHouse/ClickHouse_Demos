"""Typed retail inputs shared by SQL analytics, delivery, and learner approval."""
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from .contracts import Contract, PositiveInt, NonnegativeInt

NonnegativeDecimal = Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]


class RetailSubject(Contract):
    store_id: int = Field(ge=1, le=5000, strict=True)
    sku: int = Field(ge=1, le=100000, strict=True)


class PriceInput(Contract):
    subject: RetailSubject
    cutoff: AwareDatetime
    expires_at: AwareDatetime
    stock_units: NonnegativeInt
    current_price_minor: PositiveInt
    unit_cost_minor: NonnegativeInt
    selling_cost_minor: NonnegativeInt
    disposal_cost_minor: NonnegativeInt
    price_version: PositiveInt
    inventory_version: PositiveInt
    rule_version: PositiveInt
    sales_sequence: NonnegativeInt
    model_version: str = Field(min_length=1, max_length=128)
    base_demand_units: NonnegativeDecimal
    elasticity: Decimal = Field(le=0, allow_inf_nan=False)
    permitted_prices_minor: tuple[PositiveInt, ...] = Field(min_length=1, max_length=100)
    demand_multipliers: tuple[NonnegativeDecimal, ...] = Field(min_length=1, max_length=32)
    scenario_weights: tuple[NonnegativeDecimal, ...] = Field(min_length=1, max_length=32)

    @model_validator(mode='after')
    def coherent_comparison(self):
        if self.expires_at <= self.cutoff:
            raise ValueError('Choose inventory that expires after the analysis time.')
        prices = self.permitted_prices_minor
        if len(prices) != len(set(prices)):
            raise ValueError('Each permitted price must appear once.')
        if self.current_price_minor not in prices or max(prices) > self.current_price_minor:
            raise ValueError('Include the current price and only permitted markdowns.')
        if len(self.demand_multipliers) != len(self.scenario_weights):
            raise ValueError('Each demand scenario needs one weight.')
        if sum(self.scenario_weights) != Decimal(1):
            raise ValueError('Demand scenario weights must sum to one.')
        return self


class PriceResult(Contract):
    price_minor: PositiveInt
    units: NonnegativeDecimal
    receipts_minor: NonnegativeDecimal
    waste_units: NonnegativeDecimal
    net_recovery_minor: Decimal
    contribution_minor: Decimal
    incremental_profit_minor: Decimal
    recommended: bool


class RetailBatch(Contract):
    run_id: UUID
    batch_id: UUID
    sequence: PositiveInt
    payload_sha256: str = Field(pattern=r'^[a-f0-9]{64}$')
    event_count: int = Field(ge=0,strict=True)
    transaction_count: int = Field(ge=0,strict=True)
    logical_end: AwareDatetime
    created_at: AwareDatetime

    @model_validator(mode='after')
    def transaction_counts(self):
        if self.transaction_count > self.event_count:
            raise ValueError('Each transaction must contain at least one sales line.')
        return self


class RetailApproval(Contract):
    proposal_id: UUID
    idempotency_key: UUID


class RetailReceipt(Contract):
    receipt_id: UUID
    proposal_id: UUID
    run_id: UUID
    subject: RetailSubject
    price_minor: PositiveInt
    price_version: PositiveInt
    acknowledged_at: AwareDatetime
    effective_sequence: NonnegativeInt
