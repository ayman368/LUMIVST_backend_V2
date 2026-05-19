"""
calculations.py
===============
Translates every Excel formula in Market_Pulse.xlsx (columns I → BP)
into pure Python.  No pandas, no numpy — stdlib only.

Call:
    signals = compute_signals(today: OHLCVInput, history: list[HistoryRow])
    record  = build_record(today, signals)
    db.add(MarketPulse(**record))

Excel column reference (row 2 = most-recent day, row 3 = previous day)
───────────────────────────────────────────────────────────────────────
A=Date  B=Open  C=High  D=Low  E=Close  F=Volume  G=Value  H=Trades

I   Change                =IFERROR(E2-E3,"")
J   Change %              =IFERROR(I2/E3,"")
K   Volume change %       =IFERROR(F2/F3-100%,"")        ← 100% = 1.0
L   EMA 21                =((E2-L3)*0.0909)+L3           ← 2/(21+1)≈0.0909
M   SMA 50                =AVERAGE(E2:E51)
N   SMA 150               =AVERAGE(E2:E151)
O   SMA 200               =AVERAGE(E2:E201)
P   Market Pulse          IF/AND chain on D,E,L,M
Q   Buy Switch            =IF(T2="FTD","ON","")
R   RD                    =IF(J2>0,"RD",IF(AND(J2<0,(E2/(C2+D2))>0.5),"PRD",""))
S   RD-Count              manual (not a formula)
T   FTD                   =IF(AND(J2>=BP2,K2>0),"FTD","")
U   FTD Low               =IFERROR(LOOKUP(2,1/(T2:T26<>""),D2:D26),0)  ← last FTD low
V   RD Low                =IFERROR(LOOKUP(2,1/(S2:S26<>""),D2:D26),0)  ← last RD-Count low
W   FTD Undercut          =IF(D2<U2,"S1","")
X   Failed Rally Attempt  =IF(D2<V2,"S2","")
Y   21 Day Undercut       =IF(AX2<=-0.2%,"S5","")
Z   Overdue Break 21MA    =IF(AX2<=-0.2%,"S6","")
AA  Trending Below 21MA   =IF(J2<0,IF(C2<L2,"S7",""),"")
AB  Living Below 21MA     =IF(J2<0,IF(C2<L2,"S8",""),"")
AC  Break Below 50MA      =IF(AND(E<M,E>=mid,|E-M|/M<=0.01),"",IF(E<M,"S9",""))
AD  FTD (Buy B1)          =IF(T2="FTD","B1","")
AE  Additional FTD        manual
AF  Low Above 21MA        =IF(J2>=0,IF(D2>L2,"B3",""),"")
AG  Trending Above 21MA   =IF(J2>=0,IF(D2>L2,"B4",""),"")
AH  Living Above 21MA     =IF(J2>=0,IF(D2>L2,"B5",""),"")
AI  Low Above 50MA        =IF(J2>=0,IF(D2>M2,"B6",""),"")
AJ  S11                   array: high==MAX(65d), close in lower 25%, close<open, wide range
AK  DD&SD                 =IF(AND(J<=-0.002,K>0),"DD",IF(AND(J>0,close_pct<0.5),"SD",""))
AL  Distribution days     LET formula: COUNTIF DD+SD in window, resets after FTD
AM  Cluster               =COUNTIF(AK2:AK9,"DD")+COUNTIF(AK2:AK9,"SD")
AN  Distribution days     =COUNTIF(AK2:AK26,"DD")+COUNTIF(AK2:AK26,"SD")
AQ  Current Outlook       full IF chain: FTD/DD/RD/PRD/SD
AR  Distribution days 2   =COUNTIF(AQ2:AQ26,"DD")+COUNTIF(AQ2:AQ26,"SD")
AS  Cluster 1             =COUNTIF(AQ2:AQ9,"DD")+COUNTIF(AQ2:AQ9,"SD")
AT  Dist day fall of      =IFERROR(E2/E26-100%,0)        ← E26 = 25 days ago
AU  Year                  =YEAR(A2)
AV  Month                 =IF(AU2=2024,MONTH(A2),"")
AX  21 Day v Close        =E2/L2-1
AY  ATR%                  =IFERROR(AZ2/E2,0)
AZ  ATR                   =(AZ3*13+BA2)/14               ← Wilder 14-period
BA  TR                    =MAX(BB2:BD2)
BB  High minus low        =C2-D2
BC  High minus prev close =ABS(C2-E3)
BD  Prev close minus low  =ABS(E3-D2)
BE  OPN+Close             =SUM(C2:D2)  → high+low
BF  Close %               =IFERROR(E2/BE2,0)
BG  0.0125                constant (not stored)
BH  -0.002                constant DD_THRESHOLD
BO  MV                    =AVERAGEIF(J2:J201,">0%")
BP  FTD-R                 tiered lookup on MV
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date
from statistics import mean
from typing import Optional
from sqlalchemy.orm import Session
from app.models.tasi_settings import TasiSettings


# ─────────────────────────────────────────────────────────────────────────────
# Constants  (Excel BG1=0.0125 / BH1=-0.002)
# ─────────────────────────────────────────────────────────────────────────────
DD_THRESHOLD: float = -0.002       # BH1 — change_pct threshold for Distribution Day
EMA_MULT:     float = 0.0909       # Excel uses exactly 0.0909

# S11 range thresholds
S11_AVG_RANGE_LOW:  float = 0.0075
S11_RANGE_NARROW:   float = 0.0100
S11_RANGE_WIDE:     float = 0.0175


# ─────────────────────────────────────────────────────────────────────────────
# Input types
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class OHLCVInput:
    """Raw bar from your scraper (columns A–H)."""
    date:          date
    open:          float
    high:          float
    low:           float
    close:         float
    volume_traded: float
    value_traded:  Optional[float] = None
    no_of_trades:  Optional[int]   = None


@dataclass
class HistoryRow:
    """
    One DB row — only fields needed by calculations.
    Pass newest-first:  history[0] = yesterday, history[199] = 200 days ago.
    """
    close:          float
    high:           float
    low:            float
    volume_traded:  float
    ema_21:         Optional[float]   # L column — needed for EMA rolling
    atr:            Optional[float]   # AZ column — needed for Wilder ATR
    rd_count:       Optional[int]     # S column  — manual, used for RD Low lookup
    ftd:            Optional[str]     # T column  — "FTD" / None
    dd_sd:          Optional[str]     # AK column — "DD" / "SD" / None
    current_outlook:Optional[str]     # AQ column — used for dist_days_2, cluster_1
    change_pct:     Optional[float]   # J column  — used for MV average


@dataclass
class CalcSettings:
    """User-configurable settings that affect logic."""
    buy_switch: bool = True
    disposal_days: int = 25

def get_calc_settings(db: Session) -> CalcSettings:
    """Fetches TasiSettings from DB and returns a CalcSettings dataclass."""
    row = db.query(TasiSettings).first()
    if row:
        return CalcSettings(
            buy_switch=row.buy_switch,
            disposal_days=row.disposal_days
        )
    return CalcSettings()


# ─────────────────────────────────────────────────────────────────────────────
# Output dataclass  (maps 1-to-1 with model columns)
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class ComputedSignals:
    # I–K
    change:            Optional[float] = None
    change_pct:        Optional[float] = None
    volume_change_pct: Optional[float] = None
    # L–O
    ema_21:  Optional[float] = None
    sma_50:  Optional[float] = None
    sma_150: Optional[float] = None
    sma_200: Optional[float] = None
    # P–S
    market_pulse: Optional[str] = None
    buy_switch:   Optional[str] = None
    rd:           Optional[str] = None
    rd_count:     Optional[int] = None   # manual — left None, set externally if needed
    # T–V
    ftd:     Optional[str]   = None
    ftd_low: Optional[float] = None
    rd_low:  Optional[float] = None
    # Sell signals W–AC, AJ
    ftd_undercut:             Optional[str] = None
    failed_rally_attempt:     Optional[str] = None
    day_undercut_21:          Optional[str] = None
    overdue_break_below_21ma: Optional[str] = None
    trending_below_21ma:      Optional[str] = None
    living_below_21ma:        Optional[str] = None
    break_below_50ma:         Optional[str] = None
    s11:                      Optional[str] = None
    # Buy signals AD–AI
    ftd_1:               Optional[str] = None
    additional_ftd:      Optional[str] = None   # manual
    low_above_21ma:      Optional[str] = None
    trending_above_21ma: Optional[str] = None
    living_above_21ma:   Optional[str] = None
    low_above_50ma:      Optional[str] = None
    # AK–AS  Distribution
    dd_sd:                    Optional[str]   = None
    distribution_days:        Optional[float] = None   # AL  LET formula
    cluster:                  Optional[float] = None   # AM  last 8
    current_outlook:          Optional[str]   = None   # AQ
    distribution_days_2:      Optional[float] = None   # AR  on AQ last 25
    cluster_1:                Optional[float] = None   # AS  on AQ last 8
    distribution_day_fall_of: Optional[float] = None   # AT
    # AU–AV
    year:  Optional[int] = None
    month: Optional[int] = None
    # AX–BF
    day_v_close_21:        Optional[float] = None
    atr_pct:               Optional[float] = None
    atr:                   Optional[float] = None
    tr:                    Optional[float] = None
    high_minus_low:        Optional[float] = None
    high_minus_prev_close: Optional[float] = None
    prev_close_minus_low:  Optional[float] = None
    opn_close:             Optional[float] = None
    close_pct:             Optional[float] = None
    # BO–BP
    mv:    Optional[float] = None
    ftd_r: Optional[float] = None


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _div(num: float, den: float) -> Optional[float]:
    return None if den == 0 else num / den


def _sma(current_close: float, history_closes: list[float], n: int) -> Optional[float]:
    """SMA using today's close + last (n-1) historical closes."""
    window = [current_close] + history_closes[: n - 1]
    return mean(window) if len(window) == n else None


# ─────────────────────────────────────────────────────────────────────────────
# Main calculation function
# ─────────────────────────────────────────────────────────────────────────────
def compute_signals(today: OHLCVInput, history: list[HistoryRow], settings: CalcSettings | None = None) -> ComputedSignals:
    """
    Parameters
    ----------
    today   : today's raw OHLCV bar
    history : DB rows ordered newest → oldest.
              history[0]  = yesterday (row 3 in Excel terms)
              history[24] = 25 days ago  (row 26 in Excel)
              history[199]= 200 days ago (row 201 in Excel)

    Returns
    -------
    ComputedSignals  — unpack into MarketPulse(**build_record(today, signals))
    """
    settings = settings or CalcSettings()
    out = ComputedSignals()

    o   = today.open
    h   = today.high
    lo  = today.low
    c   = today.close
    vol = today.volume_traded

    prev        = history[0] if history else None
    prev_close  = prev.close          if prev else None
    prev_vol    = prev.volume_traded  if prev else None
    prev_ema21  = prev.ema_21         if prev else None
    prev_atr    = prev.atr            if prev else None

    hist_closes = [r.close for r in history]

    # ── I  Change  =IFERROR(E2-E3,"") ────────────────────────────────────────
    out.change = (c - prev_close) if prev_close is not None else None

    # ── J  Change %  =IFERROR(I2/E3,"") ─────────────────────────────────────
    out.change_pct = _div(out.change, prev_close) if out.change is not None else None

    # ── K  Volume change %  =IFERROR(F2/F3-100%,"") ──────────────────────────
    # Excel: F2/F3 - 100%  where 100% = 1.0  → fraction of change
    out.volume_change_pct = (
        _div(vol, prev_vol) - 1.0 if prev_vol else None
    )

    # ── L  EMA 21  =((E2-L3)*0.0909)+L3 ─────────────────────────────────────
    if prev_ema21 is not None:
        out.ema_21 = (c - prev_ema21) * EMA_MULT + prev_ema21
    elif len(hist_closes) >= 20:
        out.ema_21 = mean([c] + hist_closes[:20])   # seed with 21-bar SMA
    else:
        out.ema_21 = c                               # not enough history

    # ── M / N / O  SMA ───────────────────────────────────────────────────────
    out.sma_50  = _sma(c, hist_closes, 50)
    out.sma_150 = _sma(c, hist_closes, 150)
    out.sma_200 = _sma(c, hist_closes, 200)

    # ── BB–BD  Price structure (needed before TR) ─────────────────────────────
    out.high_minus_low = h - lo                                          # BB
    if prev_close is not None:
        out.high_minus_prev_close = abs(h - prev_close)                  # BC
        out.prev_close_minus_low  = abs(prev_close - lo)                 # BD
    else:
        out.high_minus_prev_close = None
        out.prev_close_minus_low  = None

    # ── BA  TR  =MAX(BB2:BD2) ─────────────────────────────────────────────────
    candidates = [out.high_minus_low]
    if out.high_minus_prev_close is not None: candidates.append(out.high_minus_prev_close)
    if out.prev_close_minus_low  is not None: candidates.append(out.prev_close_minus_low)
    out.tr = max(candidates)

    # ── AZ  ATR  =(AZ3*13+BA2)/14  Wilder smoothing ──────────────────────────
    if prev_atr is not None:
        out.atr = (prev_atr * 13 + out.tr) / 14
    else:
        out.atr = out.tr   # seed on first bar

    # ── AY  ATR%  =IFERROR(AZ2/E2,0) ────────────────────────────────────────
    out.atr_pct = _div(out.atr, c) if out.atr is not None else 0.0

    # ── BE  OPN+Close  =SUM(C2:D2)  → high+low ───────────────────────────────
    out.opn_close = h + lo

    # ── BF  Close%  =IFERROR(E2/BE2,0) ──────────────────────────────────────
    out.close_pct = _div(c, out.opn_close) if out.opn_close else 0.0

    # ── AX  21 Day v Close  =E2/L2-1 ─────────────────────────────────────────
    out.day_v_close_21 = (_div(c, out.ema_21) - 1) if out.ema_21 else None

    # ── AU  Year  =YEAR(A2) ───────────────────────────────────────────────────
    out.year = today.date.year

    # ── AV  Month  ───────────────────────────────────────────────────────────
    out.month = today.date.month

    # ── AT  Distribution day fall of ─────────────────────────────────────────
    # Dynamically look back based on settings.disposal_days
    hist_idx = max(0, settings.disposal_days - 2)
    out.distribution_day_fall_of = (
        _div(c, hist_closes[hist_idx]) - 1.0 if len(hist_closes) > hist_idx else 0.0
    )

    # ── BO  MV  =AVERAGEIF(J2:J201,">0%") ────────────────────────────────────
    pos_changes: list[float] = []
    if out.change_pct and out.change_pct > 0:
        pos_changes.append(out.change_pct)
    pos_changes += [
        r.change_pct
        for r in history[:199]
        if r.change_pct is not None and r.change_pct > 0
    ]
    out.mv = mean(pos_changes) if pos_changes else None

    # ── BP  FTD-R  tiered lookup on MV ───────────────────────────────────────
    # =IF(BO<0.4%,0.7%,IF(AND(BO>0.4%,BO<0.55%),0.85%,...,IF(BO>=1%,1.245%,0)))
    mv = out.mv or 0.0
    if mv < 0.004:
        out.ftd_r = 0.007
    elif mv < 0.0055:
        out.ftd_r = 0.0085
    elif mv < 0.01:
        out.ftd_r = 0.01
    else:
        out.ftd_r = 0.01245

    ftd_r     = out.ftd_r or 0.0
    chg_pct   = out.change_pct        or 0.0
    vol_chg   = out.volume_change_pct or 0.0
    close_pct = out.close_pct         or 0.0
    dv21      = out.day_v_close_21    or 0.0
    ema21     = out.ema_21
    sma50     = out.sma_50

    # ── T  FTD  =IF(AND(J2>=BP2,K2>0),"FTD","") ─────────────────────────────
    out.ftd = "FTD" if (chg_pct >= ftd_r and vol_chg > 0) else None

    # ── U  FTD Low  =IFERROR(LOOKUP(2,1/(T2:T26<>""),D2:D26),0) ─────────────
    # Window based on settings.disposal_days (e.g., 25 rows, newest-first).
    lookback = max(0, settings.disposal_days - 1)
    out.ftd_low = 0.0
    if out.ftd == "FTD":
        out.ftd_low = lo            # today counts, may be overwritten by older
    for r in history[:lookback]:
        if r.ftd == "FTD":
            out.ftd_low = r.low     # keep overwriting → oldest FTD wins

    # ── V  RD Low  =IFERROR(LOOKUP(2,1/(S2:S26<>""),D2:D26),0) ──────────────
    out.rd_low = 0.0
    for r in history[:lookback]:
        if r.rd_count is not None:
            out.rd_low = r.low      # keep overwriting → oldest rd_count wins

    # ── W  FTD Undercut  =IF(D2<U2,"S1","") ─────────────────────────────────
    out.ftd_undercut = "S1" if (out.ftd_low and lo < out.ftd_low) else None

    # ── X  Failed Rally Attempt  =IF(D2<V2,"S2","") ──────────────────────────
    out.failed_rally_attempt = "S2" if (out.rd_low and lo < out.rd_low) else None

    # ── Y  21 Day Undercut  =IF(AX2<=-0.2%,"S5","") ─────────────────────────
    out.day_undercut_21 = "S5" if dv21 <= -0.002 else None

    # ── Z  Overdue Break Below 21MA  =IF(AX2<=-0.2%,"S6","") ────────────────
    out.overdue_break_below_21ma = "S6" if dv21 <= -0.002 else None

    # ── AA  Trending Below 21MA  =IF(J2<0,IF(C2<L2,"S7",""),"") ─────────────
    out.trending_below_21ma = (
        "S7" if (chg_pct < 0 and ema21 is not None and h < ema21) else None
    )

    # ── AB  Living Below 21MA  =IF(J2<0,IF(C2<L2,"S8",""),"") ───────────────
    out.living_below_21ma = (
        "S8" if (chg_pct < 0 and ema21 is not None and h < ema21) else None
    )

    # ── AC  Break Below 50MA ─────────────────────────────────────────────────
    # =IF(AND(E<M, E>=(C+D)/2, ABS(E-M)/M<=0.01), "", IF(E<M,"S9",""))
    if sma50 is not None and c < sma50:
        midpoint   = (h + lo) / 2
        near_sma50 = c >= midpoint and abs(c - sma50) / sma50 <= 0.01
        out.break_below_50ma = None if near_sma50 else "S9"
    else:
        out.break_below_50ma = None

    # ── P  Market Pulse ───────────────────────────────────────────────────────
    # =IF(AND(D2>M2,L2>M2),"Confirmed uptrend",
    #   IF(AND(E2>M2,E2>L2),"Uptrend under pressure",
    #     IF(AND(E2<M2,1,L2<M2),"Market in correction",
    #       IF(AND(E2<M2,1,E2<L2),"Market in correction",
    #         IF(AND(E2>M2,1,E2<L2),"Uptrend under pressure","")))))
    # Note: Excel uses ",1," as a spacer (always TRUE) — ignored here
    if sma50 is not None and ema21 is not None:
        if lo > sma50 and ema21 > sma50:
            out.market_pulse = "Confirmed uptrend"
        elif c > sma50 and c > ema21:
            out.market_pulse = "Uptrend under pressure"
        elif c < sma50 and ema21 < sma50:
            out.market_pulse = "Market in correction"
        elif c < sma50 and c < ema21:
            out.market_pulse = "Market in correction"
        elif c > sma50 and c < ema21:
            out.market_pulse = "Uptrend under pressure"
        else:
            out.market_pulse = None

    # ── R  RD  =IF(J2>0,"RD",IF(AND(J2<0,(E/(C+D))>0.5),"PRD","")) ──────────
    if chg_pct > 0:
        out.rd = "RD"
    elif chg_pct < 0 and close_pct > 0.5:
        out.rd = "PRD"
    else:
        out.rd = None

    # ── AK  DD&SD ─────────────────────────────────────────────────────────────
    # =IFERROR(IF(AND(J<=-0.002,K>0),"DD",IF(AND(J=-0.002,K>0),"DD",
    #           IF(AND(J>0,(E/(C+D))<0.5),"SD","")),"")
    if (chg_pct <= DD_THRESHOLD and vol_chg > 0):
        out.dd_sd = "DD"
    elif chg_pct > 0 and close_pct < 0.5:
        out.dd_sd = "SD"
    else:
        out.dd_sd = None

    # ── AQ  Current Outlook ───────────────────────────────────────────────────
    # =IFERROR(IF(AND(J>=AY,K>0),"FTD",
    #           IF(AND(J<=-0.002,K>0),"DD",
    #             IF(AND(J>0,K>0),"RD",
    #               IF(AND(J<0,(E/(C+D))>0.5),"PRD",
    #                 IF(AND(J>0,(E/(C+D))<0.5),"SD",""))))),"")
    if chg_pct >= ftd_r and vol_chg > 0:
        out.current_outlook = "FTD"
    elif chg_pct <= DD_THRESHOLD and vol_chg > 0:
        out.current_outlook = "DD"
    elif chg_pct > 0 and vol_chg > 0:
        out.current_outlook = "RD"
    elif chg_pct < 0 and close_pct > 0.5:
        out.current_outlook = "PRD"
    elif chg_pct > 0 and close_pct < 0.5:
        out.current_outlook = "SD"
    else:
        out.current_outlook = None

    # ── Q  Buy Switch  =IF(T2="FTD","ON","") ─────────────────────────────────
    if not settings.buy_switch:
        out.buy_switch = None
    else:
        out.buy_switch = "ON" if out.ftd == "FTD" else None

    # ── AD  FTD (B1)  =IF(T2="FTD","B1","") ─────────────────────────────────
    out.ftd_1 = "B1" if out.ftd == "FTD" else None

    # ── AF  Low Above 21MA  =IF(J>=0,IF(D>L,"B3",""),"") ────────────────────
    # AG / AH same pattern, different code
    if chg_pct >= 0 and ema21 is not None and lo > ema21:
        out.low_above_21ma      = "B3"
        out.trending_above_21ma = "B4"
        out.living_above_21ma   = "B5"
    else:
        out.low_above_21ma      = None
        out.trending_above_21ma = None
        out.living_above_21ma   = None

    # ── AI  Low Above 50MA  =IF(J>=0,IF(D>M,"B6",""),"") ────────────────────
    out.low_above_50ma = (
        "B6" if (chg_pct >= 0 and sma50 is not None and lo > sma50) else None
    )

    # ── AJ  S11  (array formula) ──────────────────────────────────────────────
    # Conditions:
    #   C2 == MAX(C2:C66)         today's high is the 65-bar high
    #   E2 <= D2 + 0.25*(C2-D2)  close in lower 25% of today's range
    #   E2 < B2                   close < open
    #   (C2-D2)/D2 >= threshold   wide range day
    #     threshold = 0.01 if avg_range_50 <= 0.0075 else 0.0175
    highs_window = [h] + [r.high for r in history[:64]]
    if len(highs_window) == 65:
        is_65_high       = h == max(highs_window)
        closed_low_25pct = c <= lo + 0.25 * (h - lo)
        closed_below_open = c < o
        ranges_50 = [(r.high - r.low) / r.low for r in history[:50] if r.low > 0]
        avg_range  = mean(ranges_50) if ranges_50 else 0.0
        threshold  = S11_RANGE_NARROW if avg_range <= S11_AVG_RANGE_LOW else S11_RANGE_WIDE
        wide_range = ((h - lo) / lo) >= threshold if lo > 0 else False

        out.s11 = (
            "S11"
            if (is_65_high and closed_low_25pct and closed_below_open and wide_range)
            else None
        )

    # ── AL  Distribution days  (LET formula — exact translation) ─────────────
    # The lookback window is determined dynamically by settings.disposal_days.
    
    _window_dynamic = [{"ftd": out.ftd, "dd_sd": out.dd_sd, "rd_count": None}] + [
        {"ftd": r.ftd, "dd_sd": r.dd_sd, "rd_count": r.rd_count}
        for r in history[:lookback]
    ]

    # lastRD: oldest (highest idx) where rd_count == 1
    _last_rd_idx: Optional[int] = None
    for _j in range(len(_window_dynamic) - 1, -1, -1):
        if _window_dynamic[_j]["rd_count"] == 1:
            _last_rd_idx = _j
            break

    if _last_rd_idx is None:
        # No RD-Count anchor → simple COUNTIF full window
        out.distribution_days = float(
            sum(1 for r in _window_dynamic if r["dd_sd"] in ("DD", "SD"))
        )
    else:
        # ftdRow: oldest FTD between window[0] and window[last_rd_idx]
        _ftd_row_idx: Optional[int] = None
        for _j in range(_last_rd_idx, -1, -1):
            if _window_dynamic[_j]["ftd"] == "FTD":
                _ftd_row_idx = _j
                break

        if _ftd_row_idx is None:
            # No FTD before lastRD → full window COUNTIF
            out.distribution_days = float(
                sum(1 for r in _window_dynamic if r["dd_sd"] in ("DD", "SD"))
            )
        elif _ftd_row_idx == 0:
            # Today IS the FTD → reset to 0
            out.distribution_days = 0.0
        else:
            # Count only rows newer than ftdRow (window[0 .. ftdRow_idx-1])
            out.distribution_days = float(
                sum(1 for r in _window_dynamic[:_ftd_row_idx] if r["dd_sd"] in ("DD", "SD"))
            )

    # ── AM  Cluster  =COUNTIF(AK2:AK9,"DD")+COUNTIF(AK2:AK9,"SD") ───────────
    # AK2:AK9 = today's dd_sd + hist[0..6]  (8 rows including today)
    _window_8 = [out.dd_sd] + [r.dd_sd for r in history[:7]]
    out.cluster = float(sum(1 for v in _window_8 if v in ("DD", "SD")))

    # ── AR  Distribution days 2  =COUNTIF(AQ2:AQ26,"DD")+COUNTIF(AQ2:AQ26,"SD")
    _window_ol = [out.current_outlook] + [r.current_outlook for r in history[:lookback]]
    out.distribution_days_2 = float(sum(1 for v in _window_ol if v in ("DD", "SD")))

    # ── AS  Cluster 1  =COUNTIF(AQ2:AQ9,"DD")+COUNTIF(AQ2:AQ9,"SD") ─────────
    # AQ2:AQ9 = today's current_outlook + hist[0..6]  (8 rows including today)
    _window_ol8 = [out.current_outlook] + [r.current_outlook for r in history[:7]]
    out.cluster_1 = float(sum(1 for v in _window_ol8 if v in ("DD", "SD")))

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Merge OHLCVInput + ComputedSignals → flat dict for ORM
# ─────────────────────────────────────────────────────────────────────────────
def build_record(today: OHLCVInput, signals: ComputedSignals) -> dict:
    """
    Returns a dict ready for:
        record = MarketPulse(**build_record(today, signals))
        db.add(record)
    """
    base = {
        "date":          today.date,
        "open":          today.open,
        "high":          today.high,
        "low":           today.low,
        "close":         today.close,
        "volume_traded": today.volume_traded,
        "value_traded":  today.value_traded,
        "no_of_trades":  today.no_of_trades,
    }
    base.update(asdict(signals))
    return base