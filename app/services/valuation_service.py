"""
ValuationService
================
All calculation logic for the 8-tab valuation system.
No HTTP calls here — only DB reads and pure math.

Dependencies:
    pip install numpy-financial
"""

import statistics
import logging
from datetime import date, timedelta
from typing import Optional

import numpy_financial as npf
from sqlalchemy.orm import Session
from sqlalchemy import func, extract

from app.core.database import SessionLocal
from app.models.economic_indicators import EconomicIndicator, SP500History, TreasuryYieldCurve
from app.models.eps_estimates import EpsEstimate
from app.models.system_config import SystemConfig
from app.models.valuation_zones import ValuationZone
from app.models.tasi_components import TasiComponent

logger = logging.getLogger(__name__)


# ── Config helpers ────────────────────────────────────────────────────────────

def _get_config(db: Session, key: str, default=None):
    """Fetch a single config value from system_config and cast it."""
    row = db.query(SystemConfig).filter(SystemConfig.key == key).first()
    if row is None:
        return default
    return row.get_typed_value()


def _get_latest_indicator(db: Session, indicator_code: str) -> Optional[float]:
    """Return the most recent value for a FRED indicator."""
    row = (
        db.query(EconomicIndicator)
          .filter(EconomicIndicator.indicator_code == indicator_code)
          .order_by(EconomicIndicator.report_date.desc())
          .first()
    )
    return float(row.value) if row else None


def _get_latest_treasury(db: Session) -> Optional[object]:
    """Return the most recent TreasuryYieldCurve row."""
    return (
        db.query(TreasuryYieldCurve)
          .order_by(TreasuryYieldCurve.report_date.desc())
          .first()
    )


# ── TVM (IRR) solver ─────────────────────────────────────────────────────────

def _calculate_irr(pv: float, pmt: float, fv: float, n: int) -> Optional[float]:
    """
    Solve for annualised return given:
        pv  — current price (positive, treated as cost)
        pmt — annual dividend / cash flow
        fv  — target price at end of horizon
        n   — holding period in years

    Returns annualised rate as a percentage, or None if no solution found.
    Uses numpy_financial.irr() on the cash-flow stream.
    """
    if pv <= 0 or n <= 0:
        return None

    # Cash flow stream: [-pv, pmt, pmt, ..., pmt + fv]
    cash_flows = [-pv] + [pmt] * (n - 1) + [pmt + fv]

    try:
        rate = npf.irr(cash_flows)
        if rate is None or rate != rate:   # NaN check
            return None
        return round(float(rate) * 100, 4)
    except Exception:
        return None


# ── Bond Dashboard ────────────────────────────────────────────────────────────

class ValuationService:

    def get_bond_dashboard(self) -> dict:
        """
        Tab 1: Bond Dashboard
        Aggregates all key macro signals into one response object.
        """
        db = SessionLocal()
        try:
            sp500 = (
                db.query(SP500History)
                  .order_by(SP500History.trade_date.desc())
                  .first()
            )
            treasury = _get_latest_treasury(db)

            a_yield   = _get_latest_indicator(db, "BAMLC0A3CAEY")
            bbb_yield = _get_latest_indicator(db, "BAMLC0A4CBBBEY")
            bb_yield  = _get_latest_indicator(db, "BAMLH0A1HYBBEY")
            b_yield   = _get_latest_indicator(db, "BAMLH0A2HYBEY")
            sp500_ey  = _get_latest_indicator(db, "SP500_EY")
            unrate    = _get_latest_indicator(db, "UNRATE")
            
            payems_rows = db.query(EconomicIndicator).filter(EconomicIndicator.indicator_code == "PAYEMS").order_by(EconomicIndicator.report_date.desc()).limit(2).all()
            if len(payems_rows) >= 2:
                payems = float(payems_rows[0].value) - float(payems_rows[1].value)
            elif len(payems_rows) == 1:
                payems = float(payems_rows[0].value)
            else:
                payems = None
                
            ic4wsa    = _get_latest_indicator(db, "IC4WSA")

            yield_10y = float(treasury.year_10) if treasury and treasury.year_10 else None
            yield_2y  = float(treasury.year_2) if treasury and treasury.year_2 else None
            spread    = round(yield_10y - yield_2y, 4) if (yield_10y and yield_2y) else None

            growth_ksa = _get_config(db, "growth_ksa", 4.0)

            # Derived ratios
            sp_ey_a   = round(sp500_ey / (a_yield / 100), 4) if (sp500_ey and a_yield) else None
            sp_ey_bbb = round(sp500_ey / (bbb_yield / 100), 4) if (sp500_ey and bbb_yield) else None

            return {
                "sp500_price":        float(sp500.close) if sp500 else None,
                "sp500_pe":           float(sp500.pe_ratio) if sp500 and sp500.pe_ratio else None,
                "sp500_ey":           round(sp500_ey * 100, 4) if sp500_ey else None,  # as %
                "a_yield":            a_yield,
                "bbb_yield":          bbb_yield,
                "bb_yield":           bb_yield,
                "b_yield":            b_yield,
                "sp_ey_a_ratio":      sp_ey_a,
                "sp_ey_bbb_ratio":    sp_ey_bbb,
                "unemployment":       unrate,
                "nonfarm_payrolls":   payems,
                "initial_claims_4wma":ic4wsa,
                "yield_10y":          yield_10y,
                "yield_2y":           yield_2y,
                "spread_10y_2y":      spread,
                "dividend_yield":     _get_latest_indicator(db, "SP500_DIV_YIELD"),
                "growth_ksa":         growth_ksa,
                "as_of_date":         sp500.trade_date.isoformat() if sp500 else None,
            }
        finally:
            db.close()

    def get_valuation_copy_sheet(self, limit: int = 10) -> dict:
        """
        Returns history arrays for each column to mimic the Excel 'Valuation - Copy' side-by-side tables.
        """
        db = SessionLocal()
        try:
            def _get_hist(code):
                rows = db.query(EconomicIndicator).filter(EconomicIndicator.indicator_code == code).order_by(EconomicIndicator.report_date.desc()).limit(limit).all()
                return [{"date": r.report_date.isoformat(), "value": float(r.value)} for r in rows]

            def _get_hist_diff(code):
                rows = db.query(EconomicIndicator).filter(EconomicIndicator.indicator_code == code).order_by(EconomicIndicator.report_date.desc()).limit(limit + 1).all()
                diff_hist = []
                for i in range(len(rows) - 1):
                    diff = float(rows[i].value) - float(rows[i+1].value)
                    diff_hist.append({"date": rows[i].report_date.isoformat(), "value": diff})
                return diff_hist

            sp500_rows = db.query(SP500History).order_by(SP500History.trade_date.desc()).limit(limit).all()
            sp500_hist = [{"date": r.trade_date.isoformat(), "value": float(r.close)} for r in sp500_rows]
            
            treasury_rows = db.query(TreasuryYieldCurve).order_by(TreasuryYieldCurve.report_date.desc()).limit(limit).all()
            spread_hist = []
            for r in treasury_rows:
                if r.year_10 is not None and r.year_2 is not None:
                    spread_hist.append({"date": r.report_date.isoformat(), "value": round(float(r.year_10) - float(r.year_2), 4)})

            growth_ksa = _get_config(db, "growth_ksa", 4.0)

            return {
                "a_yield": _get_hist("BAMLC0A3CAEY"),
                "bbb_yield": _get_hist("BAMLC0A4CBBBEY"),
                "a_oas": _get_hist("BAMLC0A3CA"),
                "bbb_oas": _get_hist("BAMLC0A4CBBB"),
                "bb_yield": _get_hist("BAMLH0A1HYBBEY"),
                "b_yield": _get_hist("BAMLH0A2HYBEY"),
                "sp500_price": sp500_hist,
                "unemployment": _get_hist("UNRATE"),
                "nonfarm": _get_hist_diff("PAYEMS"),
                "claims": _get_hist("IC4WSA"),
                "spread": spread_hist,
                "growth_ksa": growth_ksa,
                "sp_ey": _get_hist("SP500_EY"),
                "dividend": _get_hist("SP500_DIV_YIELD"),
                "fed_rate": _get_hist("FEDFUNDS"),
                "banks_balance_sheet": _get_hist("TLAACBW027SBOG"),
                "fed_balance_sheet": _get_hist("WALCL"),
                "fx_reserves": _get_hist("TREAST"),
                "inflation": _get_hist("CPIAUCSL_PC1"),
                "loans": _get_hist("TOTLL"),
                "m0": _get_hist("BOGMBASE"),
                "m1": _get_hist("M1SL"),
                "m2": _get_hist("M2SL"),
            }
        finally:
            db.close()

    # ── SP-500 Valuation Scenarios ────────────────────────────────────────────

    def calculate_sp500_scenarios(self, n_years: int = 2) -> dict:
        """
        Tab 5: SP-Vlu — 10 Fair Value scenarios with TVM returns.
        Always uses 2027 EPS.
        PMT (dividend) is calculated dynamically from SP500_DIV_YIELD * current_price.
        Also includes "Interest rate adjustment due to Fed cut" scenario and G/S/B summary.
        """
        db = SessionLocal()
        try:
            sp500 = db.query(SP500History).order_by(SP500History.trade_date.desc()).first()
            if sp500 is None:
                raise ValueError("No S&P 500 price data available")

            current_price = float(sp500.close)
            
            latest_pe_record = db.query(SP500History).filter(SP500History.pe_ratio.isnot(None)).order_by(SP500History.trade_date.desc()).first()
            current_pe = float(latest_pe_record.pe_ratio) if latest_pe_record else None

            # Always use 2027 EPS for FV scenarios
            eps_year = 2027
            eps_row = db.query(EpsEstimate).filter(EpsEstimate.year == eps_year).first()
            if eps_row is None:
                raise ValueError(f"No EPS estimate for year {eps_year}")
            eps = float(eps_row.value)

            a_yield_pct  = _get_latest_indicator(db, "BAMLC0A3CAEY")
            bbb_yield_pct = _get_latest_indicator(db, "BAMLC0A4CBBBEY")
            a_yield   = (a_yield_pct or 5.0) / 100.0
            bbb_yield = (bbb_yield_pct or 5.34) / 100.0

            dividend_yield_pct = _get_latest_indicator(db, "SP500_DIV_YIELD") or 1.07
            annual_div = round((dividend_yield_pct / 100.0) * current_price, 2)

            pe_stats = self._get_historical_pe_stats(db)
            pe_min    = pe_stats["min"]
            pe_median = pe_stats["median"]
            pe_avg    = pe_stats["average"]

            pe_15 = _get_config(db, "sp500_fixed_pe_15", 15)
            pe_17 = _get_config(db, "sp500_fixed_pe_17", 17)
            pe_20 = _get_config(db, "sp500_fixed_pe_20", 20)
            pe_25 = _get_config(db, "sp500_fixed_pe_25", 25)

            fed_rate_current_pct = _get_config(db, "fed_rate_current", 3.75)
            fed_rate_expected_pct = _get_config(db, "fed_rate_expected", 3.50)
            
            pe_fed_current = round(1 / (fed_rate_current_pct / 100), 4) if fed_rate_current_pct else None
            pe_fed_expected = round(1 / (fed_rate_expected_pct / 100), 4) if fed_rate_expected_pct else None

            scenario_definitions = [
                {"name": "FV-1 (BBB)",             "pe": round(1 / bbb_yield, 4)},
                {"name": "FV-2 (A)",               "pe": round(1 / a_yield, 4)},
                {"name": "FV-3 (Current P/E)",     "pe": current_pe},
                {"name": "Current Fed Rate",       "pe": pe_fed_current},
                {"name": "Expected Fed Rate",      "pe": pe_fed_expected},
                {"name": "FV-4 (Min)",             "pe": pe_min},
                {"name": "FV-5 (Median)",          "pe": pe_median},
                {"name": "FV-6 (Avg)",             "pe": pe_avg},
                {"name": "FV-7 (P/E15)",           "pe": pe_15},
                {"name": "FV-8 (P/E17)",           "pe": pe_17},
                {"name": "FV-9 (P/E20)",           "pe": pe_20},
                {"name": "FV-10 (P/E25)",          "pe": pe_25},
            ]

            scenarios = []
            scenarios_map = {}
            for s in scenario_definitions:
                pe = s["pe"]
                if pe is None or pe <= 0:
                    continue

                fv         = round(eps * pe, 2)
                upside_pct = round((fv - current_price) / current_price * 100, 2)
                ey         = round(1 / pe * 100, 4)           # as %
                ey_a_ratio = round((1 / pe) / a_yield, 4)

                irr = _calculate_irr(current_price, annual_div, fv, n_years)

                scenario_obj = {
                    "name":          s["name"],
                    "pe":            round(pe, 2),
                    "earnings_yield":ey,
                    "fair_value":    fv,
                    "ey_a_ratio":    ey_a_ratio,
                    "upside_pct":    upside_pct,
                    f"return_{n_years}y": irr,
                }
                scenarios.append(scenario_obj)
                scenarios_map[s["name"]] = scenario_obj

            # Interest rate adjustment due to Fed cut
            adj_bbb_yield = bbb_yield / (fed_rate_current_pct / 100) * (fed_rate_expected_pct / 100) if fed_rate_current_pct else bbb_yield
            adj_a_yield = a_yield / (fed_rate_current_pct / 100) * (fed_rate_expected_pct / 100) if fed_rate_current_pct else a_yield
            
            adj_pe_bbb = round(1 / adj_bbb_yield, 4) if adj_bbb_yield > 0 else None
            adj_pe_a = round(1 / adj_a_yield, 4) if adj_a_yield > 0 else None
            
            scenarios_adj = []
            for name, adj_pe in [("FV-1 Adjusted (BBB)", adj_pe_bbb), ("FV-2 Adjusted (A)", adj_pe_a)]:
                if adj_pe:
                    fv = round(eps * adj_pe, 2)
                    irr = _calculate_irr(current_price, annual_div, fv, n_years)
                    scenarios_adj.append({
                        "name": name,
                        "pe": round(adj_pe, 2),
                        "earnings_yield": round(1 / adj_pe * 100, 4),
                        "fair_value": fv,
                        "ey_a_ratio": round((1 / adj_pe) / a_yield, 4),
                        "upside_pct": round((fv - current_price) / current_price * 100, 2),
                        f"return_{n_years}y": irr
                    })

            # Fetch Historical PE Target Prices for Gold mapping
            historical_data = self.get_historical_pe()
            target_price_2027 = None
            target_price_adj_2026 = None
            target_price_adj_2027 = None
            
            eps_2026 = historical_data["eps_estimates"].get(str(date.today().year + 1) if date.today().year == 2025 else "2026")
            if eps_2026:
                target_price_2026 = round(eps_2026 * historical_data["target_pe"], 2) if historical_data["target_pe"] else None
                target_price_adj_2026 = round(eps_2026 * historical_data["target_pe_adj"], 2) if historical_data["target_pe_adj"] else None
            
            if eps: # eps is 2027
                target_price_2027 = round(eps * historical_data["target_pe"], 2) if historical_data["target_pe"] else None
                target_price_adj_2027 = round(eps * historical_data["target_pe_adj"], 2) if historical_data["target_pe_adj"] else None

            gold = [
                target_price_adj_2026,
                target_price_2027,
                target_price_adj_2027,
                scenarios_map.get("FV-7 (P/E15)", {}).get("fair_value"),
                scenarios_map.get("FV-8 (P/E17)", {}).get("fair_value")
            ]
            silver = [
                scenarios_map.get("FV-1 (BBB)", {}).get("fair_value"),
                _calculate_irr(current_price, annual_div, scenarios_map.get("FV-2 (A)", {}).get("fair_value", 0), 2),
                scenarios_map.get("FV-9 (P/E20)", {}).get("fair_value"),
                _calculate_irr(current_price, annual_div, scenarios_adj[0]["fair_value"], 2) if len(scenarios_adj) > 0 else None,
                _calculate_irr(current_price, annual_div, scenarios_adj[1]["fair_value"], 2) if len(scenarios_adj) > 1 else None
            ]
            bronze = [
                scenarios_map.get("FV-4 (Min)", {}).get("fair_value"),
                _calculate_irr(current_price, annual_div, scenarios_map.get("FV-10 (P/E25)", {}).get("fair_value", 0), 3),
                _calculate_irr(current_price, annual_div, scenarios_map.get("FV-5 (Median)", {}).get("fair_value", 0), 3),
                _calculate_irr(current_price, annual_div, scenarios_map.get("FV-3 (Current P/E)", {}).get("fair_value", 0), 3),
                _calculate_irr(current_price, annual_div, scenarios_map.get("FV-6 (Avg)", {}).get("fair_value", 0), 3)
            ]

            # EPS estimates for display
            eps_rows = db.query(EpsEstimate).order_by(EpsEstimate.year).all()
            eps_estimates = {str(r.year): float(r.value) for r in eps_rows}

            # SP Earnings Yield
            sp_ey_pct = _get_latest_indicator(db, "SP500_EY")

            # IRR for both N=2 and N=3 for all scenarios
            scenarios_n2 = []
            scenarios_n3 = []
            for s in scenarios:
                s_n2 = dict(s)
                s_n2["irr"] = _calculate_irr(current_price, annual_div, s["fair_value"], 2)
                scenarios_n2.append(s_n2)
                s_n3 = dict(s)
                s_n3["irr"] = _calculate_irr(current_price, annual_div, s["fair_value"], 3)
                scenarios_n3.append(s_n3)

            # IRR scenarios (reverse: given FV=EPS*PE, find IRR for various PV starting points)
            irr_scenarios = []
            irr_pv_list = [
                target_price_2026,
                target_price_adj_2026,
                target_price_2027,
                target_price_adj_2027,
                scenarios_map.get("FV-7 (P/E15)", {}).get("fair_value"),
                scenarios_map.get("FV-8 (P/E17)", {}).get("fair_value"),
                scenarios_map.get("FV-1 (BBB)", {}).get("fair_value"),
                scenarios_map.get("FV-2 (A)", {}).get("fair_value"),
            ]
            target_fv = scenarios_map.get("FV-4 (Min)", {}).get("fair_value", 0)
            
            for i, pv_val in enumerate(irr_pv_list):
                if pv_val is None:
                    continue
                irr_val = _calculate_irr(pv_val, annual_div, target_fv, 3)
                irr_scenarios.append({
                    "name": f"Scenario {i+1}",
                    "n": 3,
                    "pv": pv_val,
                    "pmt": annual_div,
                    "fv": target_fv,
                    "irr": irr_val,
                })

            # Adjusted yield details for display
            adj_details = {
                "adj_bbb_yield_pct": round(adj_bbb_yield * 100, 2) if adj_bbb_yield else None,
                "adj_a_yield_pct": round(adj_a_yield * 100, 2) if adj_a_yield else None,
                "adj_pe_bbb": round(adj_pe_bbb, 2) if adj_pe_bbb else None,
                "adj_pe_a": round(adj_pe_a, 2) if adj_pe_a else None,
            }

            return {
                "scenarios":          scenarios,
                "scenarios_n2":       scenarios_n2,
                "scenarios_n3":       scenarios_n3,
                "scenarios_adjusted": scenarios_adj,
                "adj_details":        adj_details,
                "irr_scenarios":      irr_scenarios,
                "gold_silver_bronze": {
                    "gold":   [v for v in gold if v is not None],
                    "silver": [v for v in silver if v is not None],
                    "bronze": [v for v in bronze if v is not None]
                },
                "inputs": {
                    "eps_year":       eps_year,
                    "eps":            eps,
                    "eps_source":     eps_row.source,
                    "current_price":  current_price,
                    "current_pe":     current_pe,
                    "a_yield_pct":    a_yield_pct,
                    "bbb_yield_pct":  bbb_yield_pct,
                    "sp_ey_pct":      sp_ey_pct,
                    "fed_rate_current_pct": fed_rate_current_pct,
                    "fed_rate_expected_pct": fed_rate_expected_pct,
                    "dividend_yield_pct": dividend_yield_pct,
                    "annual_dividend":annual_div,
                    "n_years":        n_years,
                    "eps_estimates":   eps_estimates,
                },
                "historical_pe_stats": pe_stats,
            }
        finally:
            db.close()

    # ── Historical P/E stats ──────────────────────────────────────────────────

    def _get_historical_pe_stats(self, db: Session) -> dict:
        """
        Compute min, median, and average P/E for the last N years
        using yearly PE rows from SP500History.
        N is read from system_config ('sp500_pe_history_years', default 7).
        """
        n_years = int(_get_config(db, "sp500_pe_history_years", 7))
        cutoff_year = date.today().year - n_years

        # Alternatively query annual P/E directly (one row per year):
        annual_rows = (
            db.query(
                extract("year", SP500History.trade_date).label("yr"),
                func.avg(SP500History.pe_ratio).label("avg_pe"),
            )
            .filter(
                SP500History.pe_ratio.isnot(None),
                extract("year", SP500History.trade_date) > cutoff_year,
                extract("year", SP500History.trade_date) < date.today().year,
            )
            .group_by(extract("year", SP500History.trade_date))
            .all()
        )

        pe_values = [float(r.avg_pe) for r in annual_rows if r.avg_pe]

        if not pe_values:
            # Fallback to hardcoded values from the Excel when DB is empty
            pe_values = [23.16, 39.90, 24.09, 21.62, 25.14, 27.98, 27.05]

        # The Excel logic explicitly excludes the oldest year (2019) from the stats 
        # and instead includes TTM.
        pe_values_for_stats = pe_values.copy()
        if len(pe_values_for_stats) > 6:
            # Sort ascending by year to drop the oldest
            annual_rows_sorted = sorted(annual_rows, key=lambda r: r.yr)
            pe_values_for_stats = [float(r.avg_pe) for r in annual_rows_sorted[-6:] if r.avg_pe]
            if not pe_values_for_stats:
                pe_values_for_stats = [39.90, 24.09, 21.62, 25.14, 27.98, 27.05]

        sp500 = db.query(SP500History).order_by(SP500History.trade_date.desc()).first()
        if sp500 and sp500.pe_ratio:
            pe_values_for_stats.append(float(sp500.pe_ratio))

        return {
            "max":     round(max(pe_values_for_stats), 4),
            "min":     round(min(pe_values_for_stats), 4),
            "median":  round(statistics.median(pe_values_for_stats), 4),
            "average": round(statistics.mean(pe_values_for_stats), 4),
            "values":  [round(v, 2) for v in pe_values],
            "years_used": len(pe_values_for_stats),
        }

    # ── Economy Assessment ────────────────────────────────────────────────────

    def get_economy_assessment(self) -> dict:
        """
        Tab 4: Economy Assessment
        Evaluates each macro indicator against configurable thresholds
        and returns structured verdicts + current price zone.
        """
        db = SessionLocal()
        try:
            def _get_latest_indicator_row(session, code):
                return session.query(EconomicIndicator).filter(EconomicIndicator.indicator_code == code).order_by(EconomicIndicator.report_date.desc()).first()

            unrate_row  = _get_latest_indicator_row(db, "UNRATE")
            unrate = float(unrate_row.value) if unrate_row else None
            unrate_period = unrate_row.report_date.strftime("%B %Y") if unrate_row else "No data"

            payems_rows = db.query(EconomicIndicator).filter(EconomicIndicator.indicator_code == "PAYEMS").order_by(EconomicIndicator.report_date.desc()).limit(2).all()
            if len(payems_rows) >= 2:
                payems = float(payems_rows[0].value) - float(payems_rows[1].value)
                payems_period = payems_rows[0].report_date.strftime("%B %Y")
            elif len(payems_rows) == 1:
                payems = float(payems_rows[0].value)
                payems_period = payems_rows[0].report_date.strftime("%B %Y")
            else:
                payems = None
                payems_period = "No data"

            ic4wsa_row  = _get_latest_indicator_row(db, "IC4WSA")
            ic4wsa = float(ic4wsa_row.value) if ic4wsa_row else None
            ic4wsa_period = ic4wsa_row.report_date.strftime("%Y-%m-%d") if ic4wsa_row else "No data"

            a_oas   = _get_latest_indicator(db, "BAMLC0A3CA")
            bbb_oas = _get_latest_indicator(db, "BAMLC0A4CBBB")
            sp500_ey = _get_latest_indicator(db, "SP500_EY")
            a_yield  = _get_latest_indicator(db, "BAMLC0A3CAEY")
            treasury = _get_latest_treasury(db)

            sp500 = (
                db.query(SP500History)
                  .order_by(SP500History.trade_date.desc())
                  .first()
            )
            current_price = float(sp500.close) if sp500 else None

            latest_pe_record = db.query(SP500History).filter(SP500History.pe_ratio.isnot(None)).order_by(SP500History.trade_date.desc()).first()

            # Thresholds from config
            unemp_threshold  = _get_config(db, "unemployment_positive_threshold", 4.5)
            payems_threshold = _get_config(db, "payrolls_positive_threshold", 60)
            claims_threshold = _get_config(db, "initial_claims_positive_threshold", 260000)
            ey_attractive    = _get_config(db, "ey_a_ratio_attractive", 1.5)
            ey_neutral       = _get_config(db, "ey_a_ratio_neutral", 1.0)

            # EY/A ratio
            ey_a_ratio = None
            if sp500_ey and a_yield and a_yield > 0:
                ey_a_ratio = round(sp500_ey / (a_yield / 100), 4)

            # Yield curve slope
            yield_10y = float(treasury.year_10) if treasury and treasury.year_10 else None
            yield_2y  = float(treasury.year_2)  if treasury and treasury.year_2 else None
            spread    = round(yield_10y - yield_2y, 4) if (yield_10y and yield_2y) else None

            def verdict(condition: bool) -> str:
                return "Positive" if condition else "Negative"

            indicators = [
                {
                    "name":    "Unemployment Rate",
                    "name_ar": "معدل البطالة",
                    "value":   unrate,
                    "unit":    "%",
                    "verdict": verdict(unrate is not None and unrate < unemp_threshold),
                    "note":    f"{unrate}% ({unrate_period}) — below {unemp_threshold}% threshold" if unrate else "No data",
                },
                {
                    "name":    "Monthly Nonfarm Payrolls (Net Change)",
                    "name_ar": "الوظائف خارج الزراعة (التغير الصافي)",
                    "value":   payems,
                    "unit":    "thousands",
                    "verdict": verdict(payems is not None and payems > payems_threshold),
                    "note":    f"{int(payems):,} ({payems_period}) — above {int(payems_threshold):,} threshold" if payems is not None else "No data",
                },
                {
                    "name":    "Initial Claims (4-Week MA)",
                    "name_ar": "إعانة البطالة (متوسط 4 أسابيع)",
                    "value":   ic4wsa,
                    "unit":    "claims",
                    "verdict": verdict(ic4wsa is not None and ic4wsa < claims_threshold),
                    "note":    f"{int(ic4wsa):,} ({ic4wsa_period}) — below {int(claims_threshold):,} threshold" if ic4wsa else "No data",
                },
                {
                    "name":    "US Treasury Yield Curve (Shape)",
                    "name_ar": "شكل المنحنى",
                    "value":   None,
                    "unit":    "shape",
                    "verdict": "Positive",
                    "note":    "Shift up إيجابي على المدى الطويل",
                },
                {
                    "name":    "Treasury Yield Curve (10Y-2Y Spread)",
                    "name_ar": "منحنى الفائدة (فرق 10-2 سنة)",
                    "value":   spread,
                    "unit":    "%",
                    "verdict": verdict(spread is not None and spread > 0),
                    "note":    f"{spread:.3f}% — {'positive slope' if spread and spread > 0 else 'inverted'}" if spread else "No data",
                },
                {
                    "name":    "Corporate Bond Spread (A-rated OAS)",
                    "name_ar": "سبريد سندات الشركات (A)",
                    "value":   a_oas,
                    "unit":    "%",
                    "verdict": verdict(a_oas is not None and a_oas < 1.0),
                    "note":    f"{a_oas}% — {'low/positive' if a_oas and a_oas < 1.0 else 'elevated'}" if a_oas else "No data",
                },
                {
                    "name":    "Corporate Bond Spread (BBB-rated OAS)",
                    "name_ar": "سبريد سندات الشركات (BBB)",
                    "value":   bbb_oas,
                    "unit":    "%",
                    "verdict": verdict(bbb_oas is not None and bbb_oas < 1.5),
                    "note":    f"{bbb_oas}% — {'low/positive' if bbb_oas and bbb_oas < 1.5 else 'elevated'}" if bbb_oas else "No data",
                },
                {
                    "name":    "Interest Rate",
                    "name_ar": "الفائدة",
                    "value":   None,
                    "unit":    "rate",
                    "verdict": "Positive",
                    "note":    "إيجابي للاقتصاد، سلبي للأسواق وأرباح الشركات (متوقع تثبيت لعام 2026)",
                },
                {
                    "name":    "S&P 500 Earnings Yield vs A Bond (EY/A Ratio)",
                    "name_ar": "عائد أرباح S&P 500 مقارنة بسند A",
                    "value":   ey_a_ratio,
                    "unit":    "ratio",
                    "verdict": (
                        "Attractive" if ey_a_ratio and ey_a_ratio >= ey_attractive else
                        "Neutral"    if ey_a_ratio and ey_a_ratio >= ey_neutral    else
                        "Watch"
                    ),
                    "note": (
                        f"EY/A = {ey_a_ratio:.3f} — "
                        f"{'above 1.5x (attractive)' if ey_a_ratio and ey_a_ratio >= ey_attractive else 'below 1.5x'}"
                        if ey_a_ratio else "No data"
                    ),
                },
            ]

            # Price zone classification
            zones_raw = (
                db.query(ValuationZone)
                  .order_by(ValuationZone.order_seq)
                  .all()
            )
            zones = []
            for z in zones_raw:
                is_current = (
                    current_price is not None
                    and float(z.price_from) <= current_price < float(z.price_to)
                )
                zones.append({
                    "id":              z.id,
                    "label":           z.label,
                    "label_ar":        z.label_ar,
                    "price_from":      float(z.price_from),
                    "price_to":        float(z.price_to),
                    "return_pct_low":  z.return_pct_low,
                    "return_pct_high": z.return_pct_high,
                    "color_code":      z.color_code,
                    "is_current":      is_current,
                })

            pe_stats = self._get_historical_pe_stats(db)

            return {
                "indicators":     indicators,
                "sp500_zones":    zones,
                "current_price":  current_price,
                "current_pe":     float(latest_pe_record.pe_ratio) if latest_pe_record else None,
                "current_ey":     round(sp500_ey * 100, 2) if sp500_ey else None,
                "median_pe":      pe_stats["median"],
                "ey_a_ratio":     ey_a_ratio,
            }
        finally:
            db.close()

    # ── Historical P/E table ──────────────────────────────────────────────────

    def get_historical_pe(self, limit: int = 10) -> dict:
        """
        Tab 6: SP-PE
        Returns year-by-year P/E, EY, EY/A ratio, plus forward estimates
        and target price calculations.
        """
        db = SessionLocal()
        try:
            a_yield_pct = _get_latest_indicator(db, "BAMLC0A3CAEY") or 5.02
            a_yield     = a_yield_pct / 100.0

            # Adjusted yield based on Fed rates
            fed_rate_current = _get_config(db, "fed_rate_current", 3.75) / 100
            fed_rate_expected = _get_config(db, "fed_rate_expected", 3.50) / 100
            a_yield_adj = (a_yield / fed_rate_current * fed_rate_expected) if fed_rate_current > 0 else a_yield
            a_yield_3yr_avg = a_yield_adj  # Reusing variable name to match frontend expected fields

            # Annual P/E averages
            annual_pe = (
                db.query(
                    extract("year", SP500History.trade_date).label("yr"),
                    func.avg(SP500History.pe_ratio).label("pe"),
                )
                .filter(SP500History.pe_ratio.isnot(None))
                .group_by(extract("year", SP500History.trade_date))
                .order_by(extract("year", SP500History.trade_date).desc())
                .limit(limit)
                .all()
            )

            sp500_price = db.query(SP500History).order_by(SP500History.trade_date.desc()).first()
            sp500_pe = db.query(SP500History).filter(SP500History.pe_ratio.isnot(None)).order_by(SP500History.trade_date.desc()).first()
            
            current_price = float(sp500_price.close) if sp500_price else None
            current_pe    = float(sp500_pe.pe_ratio) if sp500_pe else None

            # EPS estimates for forward columns
            eps_rows = db.query(EpsEstimate).order_by(EpsEstimate.year).all()
            eps_map  = {r.year: float(r.value) for r in eps_rows}

            current_year = 2025  # The last full historical year in the dataset
            eps_current  = eps_map.get(current_year)
            eps_forward1 = eps_map.get(2026)
            eps_forward2 = eps_map.get(2027)

            # Required yield is dynamically calculated from current A yield
            required_ey = a_yield * 1.5
            target_pe   = round(1 / required_ey, 4) if required_ey > 0 else None
            
            required_ey_adj = a_yield_3yr_avg * 1.5
            target_pe_adj = round(1 / required_ey_adj, 4) if required_ey_adj > 0 else None

            target_price_2026 = round(eps_forward1 * target_pe, 2) if eps_forward1 and target_pe else None
            target_price_adj_2026 = round(eps_forward1 * target_pe_adj, 2) if eps_forward1 and target_pe_adj else None
            
            target_price_2027 = round(eps_forward2 * target_pe, 2) if eps_forward2 and target_pe else None
            target_price_adj_2027 = round(eps_forward2 * target_pe_adj, 2) if eps_forward2 and target_pe_adj else None

            rows = []
            for r in reversed(annual_pe):
                yr = int(r.yr)
                pe = float(r.pe) if r.pe else None
                if pe is None:
                    continue
                ey     = round(1 / pe * 100, 4)
                ey_a   = round((1 / pe) / a_yield, 4)
                ey_a_adj = round((1 / pe) / a_yield_3yr_avg, 4)
                rows.append({
                    "year":       yr,
                    "label":      str(yr),
                    "pe":         round(pe, 2),
                    "ey_pct":     ey,
                    "ey_a_ratio": ey_a,
                    "ey_a_ratio_adj": ey_a_adj,
                    "is_estimate": False,
                })

            # TTM (current)
            if current_pe:
                rows.append({
                    "year":       current_year,
                    "label":      "TTM",
                    "pe":         round(current_pe, 2),
                    "ey_pct":     round(1 / current_pe * 100, 4),
                    "ey_a_ratio": round((1 / current_pe) / a_yield, 4),
                    "ey_a_ratio_adj": round((1 / current_pe) / a_yield_3yr_avg, 4),
                    "is_estimate": False,
                })

            # Forward estimates
            for label, yr, eps in [
                ("2026F", 2026, eps_forward1),
                ("2027F", 2027, eps_forward2),
            ]:
                if eps and current_price:
                    pe_fwd = round(current_price / eps, 2)
                    rows.append({
                        "year":       yr,
                        "label":      label,
                        "pe":         pe_fwd,
                        "ey_pct":     round(1 / pe_fwd * 100, 4),
                        "ey_a_ratio": round((1 / pe_fwd) / a_yield, 4),
                        "ey_a_ratio_adj": round((1 / pe_fwd) / a_yield_3yr_avg, 4),
                        "is_estimate": True,
                    })

            pe_stats = self._get_historical_pe_stats(db)
            
            # P/E v Historical Deviation Table
            pe_current = current_pe
            pe_2026 = (current_price / eps_forward1) if current_price and eps_forward1 else None
            pe_2027 = (current_price / eps_forward2) if current_price and eps_forward2 else None
            
            def calc_dev(val, stat):
                if not val or not stat: return None
                return round((val / stat) - 1, 4)
                
            deviations = {
                "max": {
                    "ttm": calc_dev(pe_current, pe_stats["max"]),
                    "f2026": calc_dev(pe_2026, pe_stats["max"]),
                    "f2027": calc_dev(pe_2027, pe_stats["max"]),
                    "target": calc_dev(target_pe, pe_stats["max"]) if target_pe else None,
                    "target_adj": calc_dev(target_pe_adj, pe_stats["max"]) if target_pe_adj else None,
                },
                "min": {
                    "ttm": calc_dev(pe_current, pe_stats["min"]),
                    "f2026": calc_dev(pe_2026, pe_stats["min"]),
                    "f2027": calc_dev(pe_2027, pe_stats["min"]),
                    "target": calc_dev(target_pe, pe_stats["min"]) if target_pe else None,
                    "target_adj": calc_dev(target_pe_adj, pe_stats["min"]) if target_pe_adj else None,
                },
                "median": {
                    "ttm": calc_dev(pe_current, pe_stats["median"]),
                    "f2026": calc_dev(pe_2026, pe_stats["median"]),
                    "f2027": calc_dev(pe_2027, pe_stats["median"]),
                    "target": calc_dev(target_pe, pe_stats["median"]) if target_pe else None,
                    "target_adj": calc_dev(target_pe_adj, pe_stats["median"]) if target_pe_adj else None,
                },
                "average": {
                    "ttm": calc_dev(pe_current, pe_stats["average"]),
                    "f2026": calc_dev(pe_2026, pe_stats["average"]),
                    "f2027": calc_dev(pe_2027, pe_stats["average"]),
                    "target": calc_dev(target_pe, pe_stats["average"]) if target_pe else None,
                    "target_adj": calc_dev(target_pe_adj, pe_stats["average"]) if target_pe_adj else None,
                }
            }

            return {
                "rows":               rows,
                "a_yield_pct":        a_yield_pct,
                "a_yield_3yr_avg_pct":round(a_yield_3yr_avg * 100, 4),
                "required_ey_pct":    round(required_ey * 100, 4),
                "required_ey_adj_pct":round(required_ey_adj * 100, 4),
                "target_pe":          target_pe,
                "target_pe_adj":      target_pe_adj,
                "target_price_2026":  target_price_2026,
                "target_price_adj_2026": target_price_adj_2026,
                "target_price_2027":  target_price_2027,
                "target_price_adj_2027": target_price_adj_2027,
                "pe_stats":           pe_stats,
                "deviations":         deviations,
                "eps_estimates":      {str(k): v for k, v in eps_map.items()},
            }
        finally:
            db.close()

    # ── Treasury monthly curve ────────────────────────────────────────────────

    def get_monthly_yield_curve(self) -> dict:
        """
        Tab 3: TYC
        Returns monthly-averaged yield curve data for the last 13 months.
        """
        db = SessionLocal()
        try:
            rows = (
                db.query(
                    func.date_trunc("month", TreasuryYieldCurve.report_date).label("month_start"),
                    func.avg(TreasuryYieldCurve.month_1).label("mo_1"),
                    func.avg(TreasuryYieldCurve.month_3).label("mo_3"),
                    func.avg(TreasuryYieldCurve.month_6).label("mo_6"),
                    func.avg(TreasuryYieldCurve.year_1).label("yr_1"),
                    func.avg(TreasuryYieldCurve.year_2).label("yr_2"),
                    func.avg(TreasuryYieldCurve.year_3).label("yr_3"),
                    func.avg(TreasuryYieldCurve.year_5).label("yr_5"),
                    func.avg(TreasuryYieldCurve.year_7).label("yr_7"),
                    func.avg(TreasuryYieldCurve.year_10).label("yr_10"),
                    func.avg(TreasuryYieldCurve.year_20).label("yr_20"),
                    func.avg(TreasuryYieldCurve.year_30).label("yr_30"),
                )
                .filter(
                    TreasuryYieldCurve.report_date
                    >= date.today() - timedelta(days=13 * 31)
                )
                .group_by(func.date_trunc("month", TreasuryYieldCurve.report_date))
                .order_by(func.date_trunc("month", TreasuryYieldCurve.report_date))
                .all()
            )

            maturities = ["1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "20Y", "30Y"]
            fields     = ["mo_1","mo_3","mo_6","yr_1","yr_2","yr_3","yr_5","yr_7","yr_10","yr_20","yr_30"]

            months_data = []
            for row in rows:
                month_label = row.month_start.strftime("%b %Y")
                curve = {
                    f: round(float(getattr(row, f)), 4) if getattr(row, f) else None
                    for f in fields
                }
                months_data.append({"month": month_label, "curve": curve})

            return {
                "months":     months_data,
                "maturities": maturities,
                "fields":     fields,
            }
        finally:
            db.close()

    # ── TASI Market Weight ────────────────────────────────────────────────────

    def get_tasi_market_weight(self) -> dict:
        """
        Tab 7: Market Weight
        Computes weighted-average EPS and P/E for the TASI index.
        Applies the weight cap and produces both full and top-70 views.
        Methodology directly maps to Excel:
        - Top 70 selects the top 70 companies by count.
        - Their weights are squared (E_i^2) then re-normalized.
        - P/E = (Index_Level / COUNT) / Weighted_EPS.
        """
        db = SessionLocal()
        try:
            max_cap = _get_config(db, "tasi_max_weight_cap", 10.221976) / 100.0

            components = (
                db.query(TasiComponent)
                  .filter(TasiComponent.is_active == True)
                  .all()
            )

            tasi_level = _get_config(db, "tasi_index_level", 11900.0)
            total_count = len(components)
            index_per_company = tasi_level / total_count if total_count > 0 else 0

            total_weight_raw = 0.0
            for c in components:
                raw_w = float(c.weight_in_index or 0) / 100.0
                c._adj_weight_max = min(raw_w, max_cap)
                total_weight_raw += c._adj_weight_max

            total_eps_wtd = 0.0
            for c in components:
                c._weight_adj = c._adj_weight_max / total_weight_raw if total_weight_raw > 0 else 0.0
                c._eps = float(c.eps) if c.eps else 0.0
                c._eps_weighted = c._eps * c._weight_adj
                total_eps_wtd += c._eps_weighted
                c._is_top70 = False
                c._top70_raw = 0.0
                c._top70_adj = 0.0
                c._eps_wtd_top70 = 0.0

            pe_full = round(index_per_company / total_eps_wtd, 4) if total_eps_wtd > 0 else None

            # Sort by _weight_adj descending
            sorted_comps = sorted(components, key=lambda x: x._weight_adj, reverse=True)

            # Top 70 logic
            top70_count = min(70, total_count)
            sum_top70_sq = 0.0
            for i in range(top70_count):
                c = sorted_comps[i]
                c._is_top70 = True
                c._top70_raw = c._weight_adj * c._weight_adj
                sum_top70_sq += c._top70_raw

            total_eps_wtd_top70 = 0.0
            for c in sorted_comps:
                if c._is_top70 and sum_top70_sq > 0:
                    c._top70_adj = c._top70_raw / sum_top70_sq
                    c._eps_wtd_top70 = c._eps * c._top70_adj
                    total_eps_wtd_top70 += c._eps_wtd_top70

            pe_top70 = round(index_per_company / total_eps_wtd_top70, 4) if total_eps_wtd_top70 > 0 else None

            output_components = []
            for c in sorted_comps:
                output_components.append({
                    "symbol":           c.symbol,
                    "company_name":     c.company_name,
                    "company_name_ar":  c.company_name_ar,
                    "sector":           c.sector,
                    "current_price":    float(c.current_price) if c.current_price else None,
                    "weight_in_index":  float(c.weight_in_index) if c.weight_in_index else None,
                    "weight_adjusted":  round(c._adj_weight_max * 100, 6),
                    "weight_norm":      round(c._weight_adj * 100, 6),
                    "eps":              c._eps,
                    "pe_ratio":         float(c.pe_ratio) if c.pe_ratio else None,
                    "weighted_eps":     round(c._eps_weighted, 6),
                    "top70_raw":        round(c._top70_raw * 100, 6) if c._is_top70 else 0.0,
                    "top70_adj":        round(c._top70_adj * 100, 6) if c._is_top70 else 0.0,
                    "weighted_eps_top70": round(c._eps_wtd_top70, 6),
                    "is_in_top70":      c._is_top70,
                })

            return {
                "components": output_components,
                "summary_current": {
                    "index_adj":    round(index_per_company, 2),
                    "tasi_level":   tasi_level,
                    "weighted_eps": round(total_eps_wtd, 4),
                    "pe":           pe_full,
                    "total_components": total_count,
                    "profitable_components": sum(1 for c in components if c._eps > 0),
                },
                "summary_top70": {
                    "index_adj":    round(index_per_company, 2),
                    "tasi_level":   tasi_level,
                    "weighted_eps": round(total_eps_wtd_top70, 4),
                    "pe":           pe_top70,
                    "components_included": sum(1 for c in components if c._is_top70),
                },
            }
        finally:
            db.close()
