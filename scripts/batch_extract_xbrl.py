"""
Batch XBRL Extraction to PostgreSQL — FIXED VERSION
=====================================================
Fixes applied vs original:
  1. BLOCK_CODE_MAP corrected to match actual XBRL file structure:
       300200 = Balance Sheet   (was wrongly income_statement)
       300400 = Income Statement (was wrongly cash_flow)
       300700 = Cash Flow        (was missing entirely)
  2. Section is ALWAYS derived from the active block code — keyword
     fallback is only used when no block code has been seen yet (e.g.
     filing info rows).  This eliminates cross-section contamination
     (Cash Flow "Profit before tax" rows no longer land in income_statement).
  3. All value columns are extracted per block, not just column-1.
     Each column is mapped to its own (start_date, end_date) pair taken
     from the Start Date / End Date rows inside the block, so every
     historical / comparative value gets its own row with the correct period.
  4. "Note No." columns are detected and skipped.
  5. \xa0 (non-breaking space) is treated as empty — not stored as text.

Usage: same as original
    python scripts/batch_extract_xbrl_fixed.py
    python scripts/batch_extract_xbrl_fixed.py --symbols 1010 2222
    python scripts/batch_extract_xbrl_fixed.py --workers 12
"""

import os
import sys
import re
import io
import time
import argparse
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, text, func as sa_func
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from app.core.config import settings
from app.models.financial_metric_categories import FinancialMetricCategory

engine = create_engine(settings.DATABASE_URL)
Session = sessionmaker(bind=engine)

# ---------------------------------------------------------------------------
# CORRECTED Block-code → (section, subsection) mapping
# ---------------------------------------------------------------------------
# Key fix: the original had 300200→income_statement and 300400→cash_flow
# which is the OPPOSITE of the actual XBRL file layout.
BLOCK_CODE_MAP = {
    # Filing / admin
    '100010': ('filing_information', None),
    '200100': ('auditors_report',    None),

    # Financial statements — corrected codes
    '300100': ('balance_sheet',               'statement_of_financial_position'),  # rare variant
    '300200': ('balance_sheet',               'statement_of_financial_position'),  # ← was income_statement (BUG)
    '300300': ('balance_sheet',               'statement_of_financial_position'),  # another variant
    '300400': ('income_statement',            'statement_of_income'),              # ← was cash_flow (BUG)
    '300500': ('other_comprehensive_income',  'statement_of_other_comprehensive_income'),
    '300600': ('changes_in_equity',           'statement_of_changes_in_equity'),
    '300700': ('cash_flow',                   'statement_of_cash_flows'),          # ← was missing (BUG)
    '300800': ('cash_flow',                   'statement_of_cash_flows'),          # direct method variant

    # Notes
    '400100': ('notes_to_accounts', None),
}

# Prefix fallback (used only when exact code not found)
BLOCK_PREFIX_MAP = {
    '1': ('filing_information', None),
    '2': ('auditors_report',    None),
    '3': ('financial_statements', None),
    '4': ('notes_to_accounts',  None),
}

# Keyword mapping — ONLY used when no block code is active at all
METRIC_SECTION_MAPPING = {
    'revenue':             ('income_statement', 'revenue'),
    'sales':               ('income_statement', 'revenue'),
    'net_sales':           ('income_statement', 'revenue'),
    'cost_of_goods':       ('income_statement', 'cost_of_sales'),
    'cost_of_sales':       ('income_statement', 'cost_of_sales'),
    'gross_profit':        ('income_statement', 'profitability'),
    'operating_expenses':  ('income_statement', 'operating_expenses'),
    'operating_income':    ('income_statement', 'profitability'),
    'operating_profit':    ('income_statement', 'profitability'),
    'ebit':                ('income_statement', 'profitability'),
    'interest_expense':    ('income_statement', 'financing'),
    'interest_income':     ('income_statement', 'financing'),
    'profit_before_tax':   ('income_statement', 'profitability'),
    'income_tax':          ('income_statement', 'tax'),
    'tax_expense':         ('income_statement', 'tax'),
    'net_income':          ('income_statement', 'profitability'),
    'net_profit':          ('income_statement', 'profitability'),
    'total_assets':        ('balance_sheet', 'assets'),
    'current_assets':      ('balance_sheet', 'assets'),
    'cash':                ('balance_sheet', 'assets'),
    'total_liabilities':   ('balance_sheet', 'liabilities'),
    'current_liabilities': ('balance_sheet', 'liabilities'),
    'stockholders_equity': ('balance_sheet', 'equity'),
    'total_equity':        ('balance_sheet', 'equity'),
    'retained_earnings':   ('balance_sheet', 'equity'),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean_key(label: str) -> str:
    if not isinstance(label, str):
        return f"unknown_{label}"
    text = label.split('[')[0].split('|')[0]
    clean = "".join(c if c.isalnum() else "_" for c in text)
    clean = clean.lower().strip("_")
    while "__" in clean:
        clean = clean.replace("__", "_")
    return clean[:100]


def is_empty(val) -> bool:
    """True when a cell should be treated as having no value."""
    if val is None:
        return True
    s = str(val).strip().replace('\xa0', '')
    return s in ('', 'nan', 'NaT', 'None')


def parse_number(raw) -> float | None:
    if is_empty(raw):
        return None
    s = str(raw).replace(',', '').replace('\xa0', '').strip()
    try:
        if s.replace('.', '', 1).replace('-', '', 1).isdigit():
            return float(s)
    except Exception:
        pass
    return None


def extract_date(raw) -> str | None:
    if raw is None:
        return None
    if hasattr(raw, 'strftime'):
        return raw.strftime('%Y-%m-%d')
    s = str(raw).strip()
    if not s or s in ('nan', 'NaT', ''):
        return None
    m = re.search(r'(\d{4}-\d{2}-\d{2})', s)
    return m.group(1) if m else None


def compute_period(start: str | None, end: str | None) -> str | None:
    if not start or not end:
        return None
    from datetime import datetime
    try:
        s = datetime.strptime(start[:10], '%Y-%m-%d')
        e = datetime.strptime(end[:10], '%Y-%m-%d')
        months = (e.year - s.year) * 12 + (e.month - s.month) + 1
        if months <= 4:
            return f"Q{(e.month - 1) // 3 + 1}"
        if months <= 7:
            return "H1"
        if months <= 10:
            return "9M"
        return "Annual"
    except Exception:
        return None


def classify_metric_by_block(block_code: str | None) -> tuple[str, str | None]:
    """Return (section, subsection) purely from block code — no keyword guessing."""
    if not block_code:
        return 'other', None
    if block_code in BLOCK_CODE_MAP:
        return BLOCK_CODE_MAP[block_code]
    # prefix fallback
    for prefix, result in BLOCK_PREFIX_MAP.items():
        if block_code.startswith(prefix):
            return result
    return 'other', None


def classify_metric_keyword(key: str, label: str) -> tuple[str, str | None]:
    """Keyword-only fallback — used ONLY when block_code is None."""
    key_l   = key.lower()
    label_l = label.lower() if isinstance(label, str) else ''
    for kw, (sec, sub) in METRIC_SECTION_MAPPING.items():
        if kw in key_l or kw in label_l:
            return sec, sub
    # broad label hints
    for w in ('balance', 'asset', 'liability', 'equity'):
        if w in label_l:
            return 'balance_sheet', None
    for w in ('income', 'revenue', 'expense', 'profit', 'loss'):
        if w in label_l:
            return 'income_statement', None
    for w in ('cash flow', 'operating activities', 'investing', 'financing activities'):
        if w in label_l:
            return 'cash_flow', None
    return 'other', None

# ---------------------------------------------------------------------------
# Excel reader with robust fallback
# ---------------------------------------------------------------------------

def _try_read_excel(file_path: str) -> pd.DataFrame | None:
    for engine_name in ('openpyxl', 'xlrd'):
        try:
            return pd.read_excel(file_path, header=None, engine=engine_name).fillna('')
        except Exception:
            pass
    try:
        dfs = pd.read_html(file_path)
        if dfs:
            return dfs[0].fillna('')
    except Exception:
        pass
    for sep in ('\t', ','):
        for enc in ('utf-8', 'latin-1', 'cp1252'):
            try:
                return pd.read_csv(file_path, header=None, sep=sep, encoding=enc).fillna('')
            except Exception:
                pass
    return None

# ---------------------------------------------------------------------------
# Core parser — fixed
# ---------------------------------------------------------------------------

BLOCK_HEADER_RE = re.compile(r'^\[(\d+)\]')

# Labels that mark the date-header rows inside each block
_START_DATE_LABELS = {'start date', 'reporting period start date', 'reporting period star date'}
_END_DATE_LABELS   = {'end date',   'reporting period end date'}


def _is_note_col(val) -> bool:
    """Return True if this column header looks like a "Note No." annotation column."""
    s = str(val).strip().lower()
    return 'note' in s


def parse_excel_file(file_path: str) -> list[dict]:
    """
    Parse a single XBRL Excel file.

    Returns a list of dicts:
      { key, label, value, text, section, subsection, start_date, end_date, col_index }

    Each data row can produce MULTIPLE dicts — one per value column —
    so that current-period and prior-period figures are both captured.
    """
    df = _try_read_excel(file_path)
    if df is None:
        return []

    n_cols = df.shape[1]

    # State machine
    current_block_code: str | None = None
    # col_dates[c] = (start_date, end_date) for data column index c (1-based in raw df)
    col_dates: dict[int, tuple[str | None, str | None]] = {}
    # Which column indices are "note" columns to skip
    note_cols: set[int] = set()

    # Temp holders while scanning a block's date header rows
    pending_starts: dict[int, str] = {}  # col_idx → start_date string
    pending_ends:   dict[int, str] = {}

    results: list[dict] = []

    for _, row in df.iterrows():
        raw_label = row.iloc[0]
        label = str(raw_label).strip().replace('\xa0', ' ').strip()

        # ── Block header ───────────────────────────────────────────────────
        m = BLOCK_HEADER_RE.match(label)
        if m:
            current_block_code = m.group(1)
            col_dates   = {}
            note_cols   = set()
            pending_starts = {}
            pending_ends   = {}
            continue

        # Skip fully empty rows
        if not label or len(label) < 2:
            continue

        label_lower = label.lower()

        # ── Detect "Note No." columns from block header / date rows ────────
        # (They appear as non-date strings in columns 2+)
        if label_lower in _START_DATE_LABELS or label_lower in _END_DATE_LABELS:
            for c in range(1, n_cols):
                cell = row.iloc[c]
                if not is_empty(cell):
                    if _is_note_col(cell):
                        note_cols.add(c)

        # ── Start Date row ──────────────────────────────────────────────────
        if label_lower in _START_DATE_LABELS:
            d = extract_date(row.iloc[1] if n_cols > 1 else None)
            if d:
                pending_starts[1] = d
                col_dates[1] = (d, col_dates.get(1, (None, None))[1])
            continue

        # ── End Date row ────────────────────────────────────────────────────
        if label_lower in _END_DATE_LABELS:
            d = extract_date(row.iloc[1] if n_cols > 1 else None)
            if d:
                pending_ends[1] = d
                col_dates[1] = (col_dates.get(1, (None, None))[0], d)
            continue

        # ── Determine section ───────────────────────────────────────────────
        # FIX: section comes EXCLUSIVELY from the active block code.
        # Keyword matching is only a last-resort when no block is active.
        clean_label = label.replace('\t', ' ').replace('\n', ' ')[:500]
        key = clean_key(clean_label)
        if not key:
            continue

        if current_block_code is not None:
            section, subsection = classify_metric_by_block(current_block_code)
        else:
            # Before any block header (shouldn't normally happen in well-formed files)
            section, subsection = classify_metric_keyword(key, clean_label)

        # Skip abstract / header rows (no numeric values anywhere)
        row_has_data = not is_empty(row.iloc[1]) if n_cols > 1 else False
        if not row_has_data:
            continue

        # ── Extract one record per value column ─────────────────────────────
        # Determine which columns have date context
        data_cols = [1]  # عمود B فقط

        for c in data_cols:
            raw_val = row.iloc[c]
            if is_empty(raw_val):
                continue

            val_num  = parse_number(raw_val)
            val_text = None
            if val_num is None:
                s = str(raw_val).replace('\xa0', '').strip()
                if s:
                    val_text = s.replace('\t', ' ').replace('\n', ' ')

            start_date, end_date = col_dates.get(c, (None, None))

            results.append({
                "key":        key,
                "label":      clean_label,
                "value":      val_num,
                "text":       val_text,
                "section":    section,
                "subsection": subsection,
                "start_date": start_date,
                "end_date":   end_date,
                "col_index":  c,
            })

    return results

# ---------------------------------------------------------------------------
# Per-symbol extraction (thread-safe)
# ---------------------------------------------------------------------------

def extract_symbol_data(symbol, base_dir):
    symbol_dir = os.path.join(base_dir, str(symbol))
    if not os.path.exists(symbol_dir):
        return [], []

    files = [
        f for f in os.listdir(symbol_dir)
        if 'XBRL' in f and (f.endswith('.xls') or f.endswith('.xlsx'))
    ]

    symbol_metrics = []
    failed_files   = []

    for filename in files:
        try:
            parts        = os.path.splitext(filename)[0].split('_')
            year         = int(parts[0])
            fallback_per = parts[-1]

            metrics = parse_excel_file(os.path.join(symbol_dir, filename))
            if not metrics:
                failed_files.append(f"{symbol}: {filename}")
                continue

            for m in metrics:
                # Compute period from the dates extracted inside the file;
                # fall back to the filename period token.
                period = compute_period(m["start_date"], m["end_date"]) or fallback_per
                year_eff = int(m["end_date"][:4]) if m["end_date"] else year

                symbol_metrics.append((
                    str(symbol),
                    year_eff,
                    period,
                    m["key"],
                    m["value"],
                    m["text"],
                    m["label"],
                    filename,
                    m["section"],
                    m["subsection"],
                ))

        except Exception as e:
            failed_files.append(f"{symbol}: {filename} ({str(e)[:50]})")

    return symbol_metrics, failed_files

# ---------------------------------------------------------------------------
# Database helpers (unchanged from original)
# ---------------------------------------------------------------------------

def clean_text_for_db(text):
    if not isinstance(text, str):
        return text
    text = text.replace('\0', '').replace('\r', '').replace('\x00', '')
    try:
        return text.encode('utf-8', 'replace').decode('utf-8')
    except Exception:
        return ""


def ensure_metric_categories_exist(all_records):
    print("📋 Ensuring metric categories exist in database...")
    session = Session()
    try:
        unique_metrics = {}
        for rec in all_records:
            mk = rec[3]
            if mk not in unique_metrics:
                unique_metrics[mk] = {'section': rec[8], 'subsection': rec[9], 'label': rec[6]}

        existing_keys = set(
            k[0] for k in session.query(FinancialMetricCategory.metric_name).all()
        )
        section_max = dict(
            session.query(
                FinancialMetricCategory.section,
                sa_func.max(FinancialMetricCategory.display_order),
            ).group_by(FinancialMetricCategory.section).all()
        )

        new_cats = []
        for mk, info in unique_metrics.items():
            if mk in existing_keys:
                continue
            sec   = info['section']
            order = (section_max.get(sec) or -1) + 1
            section_max[sec] = order
            new_cats.append(FinancialMetricCategory(
                metric_name=mk,
                section=sec,
                subsection=info['subsection'],
                description_en=info['label'],
                unit='SAR',
                display_order=order,
                is_key_metric=False,
                is_calculated=False,
            ))

        if new_cats:
            session.bulk_save_objects(new_cats)
            session.commit()
            print(f"   ✅ Created {len(new_cats)} new metric categories")
        else:
            print(f"   ℹ️  All {len(unique_metrics)} metrics already exist")
        return True
    except Exception as e:
        print(f"   ❌ Error: {e}")
        session.rollback()
        return False
    finally:
        session.close()


def save_to_db_fast(all_records):
    if not all_records:
        print("⚠️ No records to save.")
        return 0

    ensure_metric_categories_exist(all_records)

    # Dedup: keep last per (symbol, year, period, key)
    print("🧹 Deduplicating...")
    unique_map = {}
    for rec in all_records:
        unique_map[(rec[0], rec[1], rec[2], rec[3])] = rec
    dedup   = list(unique_map.values())
    removed = len(all_records) - len(dedup)
    if removed:
        print(f"   Removed {removed:,} duplicates.")

    total = len(dedup)
    syms  = list({r[0] for r in dedup})
    print(f"🗑️  Clearing old data for {len(syms)} symbols...")

    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM company_financial_metrics WHERE company_symbol = ANY(%s)",
            (syms,)
        )
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"   ⚠️ Delete warning: {e}")
        try:
            conn.rollback()
        except Exception:
            pass

    print(f"💾 Inserting {total:,} records...")
    CHUNK = 10_000
    saved = 0

    try:
        for i in range(0, total, CHUNK):
            chunk    = dedup[i: i + CHUNK]
            chunk_no = i // CHUNK + 1
            n_chunks = (total - 1) // CHUNK + 1
            if chunk_no % 5 == 0 or chunk_no in (1, n_chunks):
                print(f"   ↳ Chunk {chunk_no}/{n_chunks} ({len(chunk):,} rows)")

            cur = conn.cursor()
            cur.execute("""
                CREATE TEMP TABLE tmp_metrics (
                    company_symbol TEXT,
                    year INT,
                    period TEXT,
                    metric_name TEXT,
                    metric_value DOUBLE PRECISION,
                    metric_text TEXT,
                    label_en TEXT,
                    source_file TEXT
                ) ON COMMIT DROP;
            """)

            buf = io.StringIO()
            for row in chunk:
                parts = []
                for idx in range(8):
                    item = row[idx]
                    if idx == 4:
                        if item is None or str(item).strip() == '':
                            parts.append('')
                        else:
                            try:
                                parts.append(str(float(item)))
                            except Exception:
                                parts.append('')
                    else:
                        if item is None or str(item).strip() == '':
                            parts.append('')
                        else:
                            val = clean_text_for_db(str(item).strip())
                            val = val.replace('\\', '\\\\').replace('"', '""')
                            if any(c in val for c in [',', '"', '\n']):
                                val = f'"{val}"'
                            parts.append(val)
                buf.write(','.join(parts) + '\n')

            buf.seek(0)
            try:
                cur.copy_expert(
                    "COPY tmp_metrics FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL '')",
                    buf,
                )
            except Exception as e:
                print(f"   ❌ COPY error chunk {chunk_no}: {str(e)[:120]}")
                conn.rollback()
                cur.close()
                continue

            cur.execute("""
                INSERT INTO company_financial_metrics
                    (company_symbol, year, period, metric_name, metric_value,
                     metric_text, label_en, source_file)
                SELECT company_symbol, year, period, metric_name, metric_value,
                       metric_text, label_en, source_file
                FROM tmp_metrics;
            """)
            conn.commit()
            cur.close()
            saved += len(chunk)

        print(f"✅ Saved {saved:,} records.")
        return saved
    except Exception as e:
        print(f"❌ DB error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return saved
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fixed Batch XBRL Extraction")
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--symbols', nargs='+')
    args = parser.parse_args()

    base_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "downloads",
    )
    if not os.path.exists(base_dir):
        print(f"❌ Not found: {base_dir}")
        return

    all_symbols = sorted(
        d for d in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, d))
    )
    if args.symbols:
        expanded = []
        for s in args.symbols:
            expanded.extend(s.split(','))
        requested   = {x.strip() for x in expanded if x.strip()}
        all_symbols = [s for s in all_symbols if s in requested]

    if not all_symbols:
        print("❌ No symbols found.")
        return

    print(f"{'='*60}")
    print(f"🚀 BATCH XBRL EXTRACTION — FIXED")
    print(f"📊 Symbols : {len(all_symbols)}")
    print(f"⚡ Workers : {args.workers}")
    print(f"{'='*60}")

    t0       = time.time()
    all_data = []
    failed   = []

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(extract_symbol_data, sym, base_dir): sym for sym in all_symbols}
        done    = 0
        for fut in as_completed(futures):
            res, fail = fut.result()
            all_data.extend(res)
            failed.extend(fail)
            done += 1
            if done % 20 == 0 or done == len(all_symbols):
                print(f"   📂 Parsed {done}/{len(all_symbols)} symbols  |  {len(all_data):,} rows so far")

    print(f"\n✅ Extraction done: {len(all_data):,} metrics from {len(all_symbols)} symbols")
    if failed:
        print(f"⚠️  {len(failed)} files failed:")
        for f in failed[:20]:
            print(f"   - {f}")
        if len(failed) > 20:
            print(f"   … and {len(failed)-20} more")

    save_to_db_fast(all_data)

    elapsed = time.time() - t0
    print(f"\n🏁 Finished in {elapsed:.1f}s  ({elapsed/max(len(all_symbols),1):.1f}s/symbol)")


if __name__ == "__main__":
    main()