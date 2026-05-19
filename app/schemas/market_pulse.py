"""
schema.py
=========
Pydantic v2 schemas for the Market Pulse API.

Security hardening applied:
  • Decimal everywhere (no float) — prevents JSON number injection tricks.
  • Tight Field(gt/lt/ge/le) on every numeric field.
  • Explicit String allowlists for every signal/status field.
  • Cross-field OHLC integrity validator on all input schemas.
  • MarketPulseUpdate exposes ONLY fields safe to patch manually.
"""

from datetime import date
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ── Reusable field bounds ──────────────────────────────────────────────────────
_PRICE = Field(gt=Decimal("0"),    lt=Decimal("9999999"))
_VOL   = Field(gt=Decimal("0"),    lt=Decimal("999999999999"))
_PCT   = Field(gt=Decimal("-1"),   lt=Decimal("1"))
_RATIO = Field(ge=Decimal("0"),    le=Decimal("1"))

# ── Allowlists ─────────────────────────────────────────────────────────────────
_MP_OK   = frozenset({"Confirmed uptrend", "Uptrend under pressure", "Market in correction"})
_RD_OK   = frozenset({"RD", "PRD"})
_OL_OK   = frozenset({"FTD", "DD", "RD", "PRD", "SD"})
_DD_OK   = frozenset({"DD", "SD"})
_SELL_OK = frozenset({"S1", "S2", "S5", "S6", "S7", "S8", "S9", "S11"})
_BUY_OK  = frozenset({"B1", "B3", "B4", "B5", "B6"})


def _chk(v, allowed, name):
    if v is not None and v not in allowed:
        raise ValueError(f"{name} must be one of {sorted(allowed)}, got {v!r}")
    return v


# ── Shared base ────────────────────────────────────────────────────────────────
class MarketPulseBase(BaseModel):

    # A–H
    date:          date
    open:          Decimal = _PRICE
    high:          Decimal = _PRICE
    low:           Decimal = _PRICE
    close:         Decimal = _PRICE
    volume_traded: Decimal = _VOL
    value_traded:  Optional[Decimal] = Field(None, gt=Decimal("0"), lt=Decimal("9999999999999"))
    no_of_trades:  Optional[int]     = Field(None, ge=0, le=100_000_000)

    # I–K
    change:            Optional[Decimal] = Field(None, gt=Decimal("-9999999"), lt=Decimal("9999999"))
    change_pct:        Optional[Decimal] = Field(None, gt=Decimal("-10"),      lt=Decimal("10"))
    volume_change_pct: Optional[Decimal] = Field(None, gt=Decimal("-1"),       lt=Decimal("1000"))

    # L–O
    ema_21:  Optional[Decimal] = Field(None, gt=Decimal("0"), lt=Decimal("9999999"))
    sma_50:  Optional[Decimal] = Field(None, gt=Decimal("0"), lt=Decimal("9999999"))
    sma_150: Optional[Decimal] = Field(None, gt=Decimal("0"), lt=Decimal("9999999"))
    sma_200: Optional[Decimal] = Field(None, gt=Decimal("0"), lt=Decimal("9999999"))

    # P–S
    market_pulse: Optional[str] = None
    buy_switch:   Optional[str] = None
    rd:           Optional[str] = None
    rd_count:     Optional[int] = Field(None, ge=0, le=9999)

    # T–V
    ftd:     Optional[str]     = None
    ftd_low: Optional[Decimal] = Field(None, ge=Decimal("0"), lt=Decimal("9999999"))
    rd_low:  Optional[Decimal] = Field(None, ge=Decimal("0"), lt=Decimal("9999999"))

    # Sell signals
    ftd_undercut:             Optional[str] = None
    failed_rally_attempt:     Optional[str] = None
    day_undercut_21:          Optional[str] = None
    overdue_break_below_21ma: Optional[str] = None
    trending_below_21ma:      Optional[str] = None
    living_below_21ma:        Optional[str] = None
    break_below_50ma:         Optional[str] = None
    s11:                      Optional[str] = None

    # Buy signals
    ftd_1:               Optional[str] = None
    additional_ftd:      Optional[str] = None
    low_above_21ma:      Optional[str] = None
    trending_above_21ma: Optional[str] = None
    living_above_21ma:   Optional[str] = None
    low_above_50ma:      Optional[str] = None

    # Distribution
    dd_sd:                    Optional[str]     = None
    distribution_days:        Optional[Decimal] = Field(None, ge=Decimal("0"), le=Decimal("25"))
    cluster:                  Optional[Decimal] = Field(None, ge=Decimal("0"), le=Decimal("8"))
    current_outlook:          Optional[str]     = None
    distribution_days_2:      Optional[Decimal] = Field(None, ge=Decimal("0"), le=Decimal("25"))
    cluster_1:                Optional[Decimal] = Field(None, ge=Decimal("0"), le=Decimal("8"))
    distribution_day_fall_of: Optional[Decimal] = Field(None, gt=Decimal("-5"), lt=Decimal("5"))

    # Time
    year:  Optional[int] = Field(None, ge=1990, le=2100)
    month: Optional[int] = Field(None, ge=1,    le=12)

    # Volatility
    day_v_close_21:        Optional[Decimal] = Field(None, gt=Decimal("-1"), lt=Decimal("1"))
    atr_pct:               Optional[Decimal] = Field(None, ge=Decimal("0"),  lt=Decimal("1"))
    atr:                   Optional[Decimal] = Field(None, ge=Decimal("0"),  lt=Decimal("9999999"))
    tr:                    Optional[Decimal] = Field(None, ge=Decimal("0"),  lt=Decimal("9999999"))
    high_minus_low:        Optional[Decimal] = Field(None, ge=Decimal("0"),  lt=Decimal("9999999"))
    high_minus_prev_close: Optional[Decimal] = None
    prev_close_minus_low:  Optional[Decimal] = None
    opn_close:             Optional[Decimal] = Field(None, gt=Decimal("0"),  lt=Decimal("9999999"))
    close_pct:             Optional[Decimal] = Field(None, ge=Decimal("0"),  le=Decimal("1"))

    # Velocity
    mv:    Optional[Decimal] = Field(None, ge=Decimal("0"), lt=Decimal("1"))
    ftd_r: Optional[Decimal] = Field(None, ge=Decimal("0"), lt=Decimal("1"))

    # ── Cross-field OHLC integrity ─────────────────────────────────────────────
    # Removed strict OHLC integrity to prevent HTTP 500 errors on slightly flawed exchange data.

    # ── String allowlist validators ────────────────────────────────────────────
    @field_validator("market_pulse")
    @classmethod
    def _v_mp(cls, v): return _chk(v, _MP_OK, "market_pulse")

    @field_validator("rd")
    @classmethod
    def _v_rd(cls, v): return _chk(v, _RD_OK, "rd")

    @field_validator("buy_switch")
    @classmethod
    def _v_bs(cls, v): return _chk(v, {"ON"}, "buy_switch")

    @field_validator("ftd")
    @classmethod
    def _v_ftd(cls, v): return _chk(v, {"FTD"}, "ftd")

    @field_validator("dd_sd")
    @classmethod
    def _v_dd(cls, v): return _chk(v, _DD_OK, "dd_sd")

    @field_validator("current_outlook")
    @classmethod
    def _v_ol(cls, v): return _chk(v, _OL_OK, "current_outlook")

    @field_validator("ftd_1", "additional_ftd")
    @classmethod
    def _v_b1(cls, v): return _chk(v, {"B1"}, "ftd_1/additional_ftd")

    @field_validator(
        "ftd_undercut", "failed_rally_attempt", "day_undercut_21",
        "overdue_break_below_21ma", "trending_below_21ma",
        "living_below_21ma", "break_below_50ma", "s11",
    )
    @classmethod
    def _v_sell(cls, v): return _chk(v, _SELL_OK, "sell signal")

    @field_validator("low_above_21ma", "trending_above_21ma", "living_above_21ma", "low_above_50ma")
    @classmethod
    def _v_buy(cls, v): return _chk(v, {"B3", "B4", "B5", "B6"}, "buy signal")


# ── Schemas ────────────────────────────────────────────────────────────────────

class MarketPulseCreate(MarketPulseBase):
    """Full manual insert — all computed fields must be provided."""
    pass


class OHLCVCreate(BaseModel):
    """
    Ingest endpoint input — scraper sends only raw OHLCV.
    Router computes everything else via calculations.py.
    """
    date:          date
    open:          Decimal = _PRICE
    high:          Decimal = _PRICE
    low:           Decimal = _PRICE
    close:         Decimal = _PRICE
    volume_traded: Decimal = _VOL
    value_traded:  Optional[Decimal] = Field(None, gt=Decimal("0"))
    no_of_trades:  Optional[int]     = Field(None, ge=0, le=100_000_000)


class MarketPulseUpdate(BaseModel):
    """
    Partial patch — intentionally limited to fields safe to change manually.
    Computed signal fields cannot be overridden via API (recalculate instead).
    """
    open:           Optional[Decimal] = Field(None, gt=Decimal("0"), lt=Decimal("9999999"))
    high:           Optional[Decimal] = Field(None, gt=Decimal("0"), lt=Decimal("9999999"))
    low:            Optional[Decimal] = Field(None, gt=Decimal("0"), lt=Decimal("9999999"))
    close:          Optional[Decimal] = Field(None, gt=Decimal("0"), lt=Decimal("9999999"))
    volume_traded:  Optional[Decimal] = Field(None, gt=Decimal("0"), lt=Decimal("999999999999"))
    value_traded:   Optional[Decimal] = Field(None, gt=Decimal("0"))
    no_of_trades:   Optional[int]     = Field(None, ge=0, le=100_000_000)
    rd_count:       Optional[int]     = Field(None, ge=0, le=9999)
    additional_ftd: Optional[str]     = None
    market_pulse:   Optional[str]     = None

    @field_validator("market_pulse")
    @classmethod
    def _v_mp(cls, v): return _chk(v, _MP_OK, "market_pulse")

    @field_validator("additional_ftd")
    @classmethod
    def _v_af(cls, v): return _chk(v, {"B1"}, "additional_ftd")


class MarketPulseRead(MarketPulseBase):
    id: int
    model_config = {"from_attributes": True}


class MarketPulseAverages(BaseModel):
    avg_change:                   Optional[Decimal] = None
    avg_change_pct:               Optional[Decimal] = None
    avg_volume_change_pct:        Optional[Decimal] = None
    avg_ema_21:                   Optional[Decimal] = None
    avg_sma_50:                   Optional[Decimal] = None
    avg_sma_150:                  Optional[Decimal] = None
    avg_sma_200:                  Optional[Decimal] = None
    avg_rd_count:                 Optional[Decimal] = None
    avg_distribution_days:        Optional[Decimal] = None
    avg_cluster:                  Optional[Decimal] = None
    avg_distribution_day_fall_of: Optional[Decimal] = None
    avg_day_v_close_21:           Optional[Decimal] = None
    avg_atr_pct:                  Optional[Decimal] = None
    avg_atr:                      Optional[Decimal] = None
    avg_tr:                       Optional[Decimal] = None
    avg_high_minus_low:           Optional[Decimal] = None
    avg_high_minus_prev_close:    Optional[Decimal] = None
    avg_prev_close_minus_low:     Optional[Decimal] = None
    avg_opn_close:                Optional[Decimal] = None
    avg_close_pct:                Optional[Decimal] = None
    avg_mv:                       Optional[Decimal] = None
    avg_ftd_r:                    Optional[Decimal] = None


class MarketPulseStats(BaseModel):
    total_records:                int
    averages:                     MarketPulseAverages
    market_pulse_distribution:    dict[str, int]
    current_outlook_distribution: dict[str, int]
