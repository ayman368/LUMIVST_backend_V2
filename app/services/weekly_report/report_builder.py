"""
Orchestrates all weekly report calculators and returns a JSON-serializable report.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone, timedelta
import pandas as pd
from sqlalchemy.orm import Session

from app.services.weekly_report.calculators import (
    compute_breakouts,
    compute_breakout_stock_series,
    compute_index_summary,
    compute_new_highs_lows,
    compute_rankings,
    compute_sector_analytics,
    compute_tasi_trend_series,
    compute_trend_breadth,
    compute_volume_gainers,
    get_index_trend_labels,
)
from app.services.weekly_report.data_loader import (
    load_stocks_dataframe,
    load_tasi_dataframe,
    market_cap_groups,
    tasi_weekly_return,
    trading_week_bounds,
)

logger = logging.getLogger(__name__)


def _strip_internal_keys(rows: list[dict]) -> list[dict]:
    return [{k: v for k, v in r.items() if k != "score" and k != "market_cap"} for r in rows]


def _top_market_cap(ranked: list[dict], n: int = 15) -> list[dict]:
    # Ensure we always return the top N by market cap, even if it's zero or missing
    with_cap = list(ranked)
    with_cap.sort(key=lambda x: x.get("market_cap", 0.0) or 0.0, reverse=True)
    return _strip_internal_keys(with_cap[:n])


def _aporia_trend_to_label(raw: str) -> str:
    """Convert Aporia trend format ('up:XX,YY', 'down:XX,YY', 'flat') to 'Bull'/'Bear'/'Neutral'."""
    if not raw:
        return "Neutral"
    s = str(raw).strip().lower()
    if s.startswith("up"):
        return "Bull"
    if s.startswith("down"):
        return "Bear"
    return "Neutral"


def build_weekly_report(db: Session, week_end: date) -> dict:
    week_start, week_end = trading_week_bounds(week_end)
    ws = week_start.isoformat()
    we = week_end.isoformat()

    logger.info("Building weekly report for %s → %s", ws, we)

    df = load_stocks_dataframe(db, week_end)
    df_tasi = load_tasi_dataframe(db)

    week_start_dt = pd.to_datetime(ws)
    week_end_dt = pd.to_datetime(we)
    
    # Critical: Slice data to never exceed the report's week_end
    # so that historical reports act as true snapshots in time.
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] <= week_end_dt].copy()
    
    if df_tasi is not None and not df_tasi.empty:
        df_tasi["date"] = pd.to_datetime(df_tasi["date"])
        df_tasi = df_tasi[df_tasi["date"] <= week_end_dt].copy()

    tasi_ret = tasi_weekly_return(db, week_start, week_end, df_tasi=df_tasi)
    cap_groups = market_cap_groups(df, week_end)

    # Use Top 30 and Top 50 largest companies for TASI30 and TASI50
    tasi30_symbols = cap_groups.get("Large Cap", [])[:30]
    tasi50_symbols = cap_groups.get("Large Cap", [])[:50]

    from app.services.weekly_report.data_loader import get_market_volume
    prev_week_start = (week_start - timedelta(days=7)).isoformat()
    prev_week_end = (week_start - timedelta(days=1)).isoformat()
    
    total_market_vol = get_market_volume(db, ws, we)
    prev_market_vol = get_market_volume(db, prev_week_start, prev_week_end)

    # Fetch Aporia Scraped Data
    from app.models.aporia import AporiaAnalytics
    aporia_records = db.query(AporiaAnalytics).all()
    aporia_df = pd.DataFrame([r.__dict__ for r in aporia_records])
    
    if not aporia_df.empty:
        # Clean up SQLAlchemy state
        if '_sa_instance_state' in aporia_df.columns:
            aporia_df = aporia_df.drop(columns=['_sa_instance_state'])
            
        aporia_all = aporia_df[aporia_df['filter_category'] == 'all_metrics'].copy()
        
        # Override the stock universe to match Aporia exactly!
        aporia_tickers = aporia_all['ticker'].tolist()
        df = df[df['symbol'].isin(aporia_tickers)].copy()
        
        # Build Aporia mappings for quick lookup
        aporia_map = aporia_all.set_index('ticker').to_dict('index')
    else:
        aporia_map = {}

    index_data = compute_index_summary(
        df,
        ws,
        we,
        df_tasi=df_tasi,
        tasi_return=tasi_ret,
        tasi_market_cap_groups=cap_groups,
        msci30_symbols=tasi30_symbols,
        tasi50_symbols=tasi50_symbols,
        global_indices={}, # Removed global indices as per user request
        total_market_vol=total_market_vol,
        prev_market_vol=prev_market_vol,
    )

    rankings = compute_rankings(df, ws, we)
    
    # OVERRIDE Rankings with Aporia Scraped Data!
    if aporia_map:
        for r in rankings["ranked_stocks"]:
            sym = r["symbol"]
            if sym in aporia_map:
                a = aporia_map[sym]
                
                # Convert Aporia trend format to Bull/Bear/Neutral
                r["trend_daily"] = _aporia_trend_to_label(a.get("daily_trend", ""))
                r["trend_weekly"] = _aporia_trend_to_label(a.get("weekly_trend", ""))
                r["trend_monthly"] = _aporia_trend_to_label(a.get("monthly_trend", ""))
                
                # Trend Rank
                rank_str = str(a.get("trend_rank", "")).strip()
                try:
                    r["trend_rank"] = int(rank_str)
                except (ValueError, TypeError):
                    pass
                    
                # Days Since 250-Day High
                days_str = str(a.get("days_since_high_250", "")).strip().replace(",", "")
                try:
                    r["days_since_250d_high"] = int(days_str)
                except (ValueError, TypeError):
                    pass
                    
                # % Below 250-Day High
                pct_str = str(a.get("pfh_250", "")).strip().replace('%', '').replace(",", "")
                try:
                    r["pct_below_250d_high"] = float(pct_str)
                except (ValueError, TypeError):
                    pass
                        
        # Build directly from Aporia data to guarantee 100% exact match
        local_returns = {r["symbol"]: r.get("weekly_return", 0.0) for r in rankings["ranked_stocks"]}
        
        def _build_aporia_list(category):
            cat_df = aporia_df[aporia_df['filter_category'] == category]
            if cat_df.empty: return []
            res = []
            for _, row in cat_df.iterrows():
                # Extract numbers safely
                try: rank_val = int(str(row.get("trend_rank", "")).strip())
                except: rank_val = 0
                
                try: pct_val = float(str(row.get("pfh_250", "")).strip().replace('%', '').replace(',', ''))
                except: pct_val = 0.0
                
                try: days_val = int(str(row.get("days_since_high_250", "")).strip().replace(',', ''))
                except: days_val = 0
                
                ret_val = local_returns.get(row["ticker"], 0.0)
                
                res.append({
                    "symbol": row["ticker"],
                    "stock_name": row["name"],
                    "weekly_return": ret_val,
                    "trend_daily": _aporia_trend_to_label(row.get("daily_trend", "")),
                    "trend_weekly": _aporia_trend_to_label(row.get("weekly_trend", "")),
                    "trend_monthly": _aporia_trend_to_label(row.get("monthly_trend", "")),
                    "trend_rank": rank_val,
                    "pct_below_250d_high": pct_val,
                    "days_since_250d_high": days_val,
                })
            return res

        aporia_top = _build_aporia_list('strongest_uptrends')
        if aporia_top:
            rankings["top_15"] = aporia_top[:15]
            
        aporia_bottom = _build_aporia_list('strongest_downtrends')
        if aporia_bottom:
            rankings["bottom_15"] = aporia_bottom[:15]
            
    ranked = rankings["ranked_stocks"]

    tasi_series = compute_tasi_trend_series(df_tasi)
    tasi_labels = get_index_trend_labels(df_tasi)

    breakouts_data = compute_breakouts(df, ws, we)
    
    # OVERRIDE Breakouts with Aporia Scraped Data!
    if not aporia_df.empty:
        aporia_breakouts = aporia_df[aporia_df['filter_category'] == 'breakouts']
        if not aporia_breakouts.empty:
            aporia_breakout_list = []
            for _, row in aporia_breakouts.iterrows():
                aporia_breakout_list.append({
                    "symbol": row["ticker"],
                    "stock_name": row["name"],
                    "direction": "up", # Default to up for Aporia breakouts
                    "type": "250d",
                    "breakout_type": "High",
                    "description": str(row["breakout"])
                })
            # Replace our custom breakouts list with Aporia's exact list
            breakouts_data["breakouts"] = aporia_breakout_list
            breakouts_data["count"] = len(aporia_breakout_list)

    # Calculate Aporia's Exact Current Trend Breadth
    aporia_breadth = {
        "daily": {"bull": 0, "bear": 0},
        "weekly": {"bull": 0, "bear": 0},
        "monthly": {"bull": 0, "bear": 0},
    }
    if not aporia_df.empty:
        for _, row in aporia_all.iterrows():
            for tf in ["daily", "weekly", "monthly"]:
                val = str(row.get(f"{tf}_trend", "")).lower()
                if val.startswith("up"):
                    aporia_breadth[tf]["bull"] += 1
                elif val.startswith("down"):
                    aporia_breadth[tf]["bear"] += 1

    trend_breadth_data = compute_trend_breadth(df)
    if not aporia_df.empty:
        trend_breadth_data["current"] = {
            "daily": aporia_breadth["daily"]["bull"] - aporia_breadth["daily"]["bear"],
            "weekly": aporia_breadth["weekly"]["bull"] - aporia_breadth["weekly"]["bear"],
            "monthly": aporia_breadth["monthly"]["bull"] - aporia_breadth["monthly"]["bear"],
        }
        
    # Get Aporia's Largest Market Cap List
    top_cap_list = _top_market_cap(ranked)
    if not aporia_df.empty:
        aporia_cap = _build_aporia_list('largest')
        if aporia_cap:
            top_cap_list = aporia_cap[:15]

    # Fetch Aporia charts for breakout stocks
    breakout_stocks_charts = []
    if not aporia_df.empty:
        from app.models.aporia import AporiaChart
        for brk in breakouts_data.get("breakouts", []):
            ticker = brk["symbol"]
            chart_record = db.query(AporiaChart).filter(
                AporiaChart.ticker == ticker,
                AporiaChart.chart_type == 'breakout'
            ).first()
            
            series_list = []
            labels_list = []
            if chart_record and chart_record.chart_data:
                data = chart_record.chart_data
                labels_list = data.get("dates", [])
                series_list = data.get("prices", [])
                
            breakout_stocks_charts.append({
                "symbol": ticker,
                "stock_name": brk["stock_name"],
                "series": series_list,
                "labels": labels_list,
                "breakout_type": brk["description"]
            })
    else:
        # Fallback to local computation
        from .calculators.breakouts import compute_breakout_stock_series
        breakout_stocks_charts = compute_breakout_stock_series(df, breakouts_data["breakouts"], days=None)

    report = {
        "week_label": index_data["week_label"],
        "week_start": ws,
        "week_end": we,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "index_performance": index_data["index_performance"],
        "trend_analysis": {
            "series": tasi_series,
            "current_close": tasi_labels["current_close"],
            "high_250": tasi_labels["high_250"],
            "low_250": tasi_labels["low_250"],
            "daily": tasi_labels["daily"],
            "weekly": tasi_labels["weekly"],
            "monthly": tasi_labels["monthly"],
        },
        "volume": index_data["volume"],
        "sector_analytics": compute_sector_analytics(df, ws, we),
        "trend_breadth": trend_breadth_data,
        "new_highs_lows": compute_new_highs_lows(df, df_tasi=df_tasi),
        "stock_performance": index_data["stock_performance"],
        "top_market_cap": top_cap_list,
        "breakouts": breakouts_data,
        "breakout_stocks": breakout_stocks_charts,
        "top_ranked": _strip_internal_keys(rankings["top_15"]),
        "bottom_ranked": _strip_internal_keys(rankings["bottom_15"]),
        "volume_gainers": compute_volume_gainers(df, ws, we),
    }

    logger.info(
        "Report built: %s sectors, %s breakouts, %s volume gainers",
        len(report["sector_analytics"]),
        len(report["breakouts"]["breakouts"]),
        len(report["volume_gainers"]),
    )
    return report
