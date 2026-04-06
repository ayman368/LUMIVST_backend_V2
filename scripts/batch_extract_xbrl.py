"""
Batch XBRL Extraction to PostgreSQL (Enhanced with Categorization)
===================================================================
1. Extracts data from XBRL Excel files using pandas (with robust fallback).
2. Automatically categorizes metrics into sections (Income Statement, Cash Flow, Balance Sheet).
3. Creates/updates financial metric categories in the database.
4. Separates clean numeric values from text values.
5. Uses PostgreSQL COPY protocol for ultra-fast bulk insertion.
6. Handles duplicates via UPSERT (ON CONFLICT DO UPDATE).

Usage:
    python scripts/batch_extract_xbrl.py                          # All symbols
    python scripts/batch_extract_xbrl.py --symbols 1010 2222      # Specific symbols
    python scripts/batch_extract_xbrl.py --workers 12             # More parallelism
"""

import os
import sys
import re
import pandas as pd
import argparse
import time
import io
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from app.core.config import settings
from app.models.financial_metric_categories import FinancialMetricCategory

# Database Connection
engine = create_engine(settings.DATABASE_URL)
Session = sessionmaker(bind=engine)

# ---------------------------------------------------------------------------
# Metric Classification
# ---------------------------------------------------------------------------

METRIC_SECTION_MAPPING = {
    # Income Statement
    'revenue': ('income_statement', 'revenue'),
    'sales': ('income_statement', 'revenue'),
    'net_sales': ('income_statement', 'revenue'),
    'cost_of_goods': ('income_statement', 'cost_of_sales'),
    'cost_of_sales': ('income_statement', 'cost_of_sales'),
    'gross_profit': ('income_statement', 'profitability'),
    'operating_expenses': ('income_statement', 'operating_expenses'),
    'operating_income': ('income_statement', 'profitability'),
    'operating_profit': ('income_statement', 'profitability'),
    'ebit': ('income_statement', 'profitability'),
    'interest_expense': ('income_statement', 'financing'),
    'interest_income': ('income_statement', 'financing'),
    'profit_before_tax': ('income_statement', 'profitability'),
    'income_tax': ('income_statement', 'tax'),
    'tax_expense': ('income_statement', 'tax'),
    'net_income': ('income_statement', 'profitability'),
    'net_profit': ('income_statement', 'profitability'),
    'earnings': ('income_statement', 'profitability'),

    # Balance Sheet
    'total_assets': ('balance_sheet', 'assets'),
    'current_assets': ('balance_sheet', 'assets'),
    'cash': ('balance_sheet', 'assets'),
    'cash_equivalents': ('balance_sheet', 'assets'),
    'accounts_receivable': ('balance_sheet', 'assets'),
    'inventory': ('balance_sheet', 'assets'),
    'property_plant': ('balance_sheet', 'assets'),
    'total_liabilities': ('balance_sheet', 'liabilities'),
    'current_liabilities': ('balance_sheet', 'liabilities'),
    'accounts_payable': ('balance_sheet', 'liabilities'),
    'long_term_debt': ('balance_sheet', 'liabilities'),
    'short_term_debt': ('balance_sheet', 'liabilities'),
    'stockholders_equity': ('balance_sheet', 'equity'),
    'total_equity': ('balance_sheet', 'equity'),
    'retained_earnings': ('balance_sheet', 'equity'),
    'common_stock': ('balance_sheet', 'equity'),

    # Cash Flow
    'cash_from_operations': ('cash_flow', 'operating_activities'),
    'operating_activities': ('cash_flow', 'operating_activities'),
    'depreciation': ('cash_flow', 'adjustments'),
    'amortization': ('cash_flow', 'adjustments'),
    'stock_based_compensation': ('cash_flow', 'adjustments'),
    'cash_from_investing': ('cash_flow', 'investing_activities'),
    'investing_activities': ('cash_flow', 'investing_activities'),
    'capital_expenditures': ('cash_flow', 'investing_activities'),
    'acquisitions': ('cash_flow', 'investing_activities'),
    'cash_from_financing': ('cash_flow', 'financing_activities'),
    'financing_activities': ('cash_flow', 'financing_activities'),
    'debt_payments': ('cash_flow', 'financing_activities'),
    'dividend_payments': ('cash_flow', 'financing_activities'),
    'stock_repurchases': ('cash_flow', 'financing_activities'),
}

BLOCK_CODE_MAP = {
    '100010': ('filing_information', None),
    '200100': ('auditors_report', None),
    '300100': ('balance_sheet', 'statement_of_financial_position'),
    '300200': ('income_statement', 'statement_of_income'),
    '300300': ('income_statement', 'other_comprehensive_income'),
    '300400': ('cash_flow', 'statement_of_cash_flows'),
    '300500': ('changes_in_equity', 'statement_of_changes_in_equity'),
    '400100': ('notes_to_accounts', None),
}

BLOCK_PREFIX_MAP = {
    '100': ('filing_information', None),
    '200': ('auditors_report', None),
    '300': ('income_statement', None),
    '400': ('notes_to_accounts', None),
}


def clean_key(text):
    """Generates a clean snake_case key from the label."""
    if not isinstance(text, str):
        return f"unknown_{text}"
    text = text.split('[')[0].split('|')[0]
    clean = "".join(c if c.isalnum() else "_" for c in text)
    clean = clean.lower().strip("_")
    while "__" in clean:
        clean = clean.replace("__", "_")
    return clean[:100]


def classify_metric(metric_key, metric_label, block_code=None):
    """
    Classifies a metric into (section, subsection).
    Priority: block_code exact → block_code prefix → keyword matching → fallback.
    """
    # 1. Block code exact match
    if block_code and block_code in BLOCK_CODE_MAP:
        return BLOCK_CODE_MAP[block_code]

    # 2. Block code prefix match
    if block_code:
        for prefix, result in BLOCK_PREFIX_MAP.items():
            if block_code.startswith(prefix):
                return result

    # 3. Keyword-based classification
    metric_lower = metric_key.lower()
    label_lower = metric_label.lower() if isinstance(metric_label, str) else ""

    for keyword, (section, subsection) in METRIC_SECTION_MAPPING.items():
        if keyword in metric_lower or keyword in label_lower:
            return section, subsection

    # 4. Label-based fallback
    if any(w in label_lower for w in ['balance', 'asset', 'liability', 'equity', 'statement of financial position']):
        return 'balance_sheet', None
    elif any(w in label_lower for w in ['income', 'revenue', 'expense', 'profit', 'loss', 'statement of comprehensive income']):
        return 'income_statement', None
    elif any(w in label_lower for w in ['cash flow', 'operating', 'investing', 'financing', 'statement of cash']):
        return 'cash_flow', None

    return 'other', None


# ---------------------------------------------------------------------------
# Excel Parsing (with robust fallback)
# ---------------------------------------------------------------------------

def _try_read_excel(file_path):
    """Try multiple methods to read an Excel file. Returns DataFrame or None."""
    # 1. openpyxl (modern XLSX)
    try:
        return pd.read_excel(file_path, header=None, engine='openpyxl').fillna('')
    except Exception:
        pass

    # 2. xlrd (legacy XLS)
    try:
        return pd.read_excel(file_path, header=None, engine='xlrd').fillna('')
    except Exception:
        pass

    # 3. read_html (HTML disguised as XLS)
    try:
        dfs = pd.read_html(file_path)
        if dfs:
            return dfs[0].fillna('')
    except Exception:
        pass

    # 4. CSV with tab separator
    for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
        try:
            return pd.read_csv(file_path, header=None, sep='\t', encoding=encoding).fillna('')
        except Exception:
            pass

    # 5. CSV with comma separator
    for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
        try:
            return pd.read_csv(file_path, header=None, sep=',', encoding=encoding).fillna('')
        except Exception:
            pass

    return None


def _extract_date_value(raw_value):
    """Extract a YYYY-MM-DD date string from various pandas cell formats."""
    if raw_value is None:
        return None
    # pandas may parse Excel dates as Timestamp/datetime
    if hasattr(raw_value, 'strftime'):
        return raw_value.strftime('%Y-%m-%d')
    s = str(raw_value).strip()
    if not s or s in ('', 'nan', 'NaT'):
        return None
    # Try to find YYYY-MM-DD pattern
    m = re.search(r'(\d{4}-\d{2}-\d{2})', s)
    if m:
        return m.group(1)
    return None


def _compute_period_from_dates(start_date_str, end_date_str):
    """
    Compute a period label (Q1, Q2, Q3, Q4, H1, 9M, Annual) from date strings.
    Returns None if dates can't be parsed.
    """
    if not start_date_str or not end_date_str:
        return None
    try:
        from datetime import datetime
        start = datetime.strptime(start_date_str[:10], '%Y-%m-%d')
        end = datetime.strptime(end_date_str[:10], '%Y-%m-%d')

        months = (end.year - start.year) * 12 + (end.month - start.month) + 1

        if months <= 4:
            quarter = (end.month - 1) // 3 + 1
            return f"Q{quarter}"
        elif months <= 7:
            return "H1"
        elif months <= 10:
            return "9M"
        else:
            return "Annual"
    except Exception:
        return None


# Keys that indicate Start/End Date rows in XBRL files
_DATE_START_KEYS = {'start_date', 'reporting_period_start_date', 'reporting_period_star_date'}
_DATE_END_KEYS = {'end_date', 'reporting_period_end_date'}


def parse_excel_file(file_path):
    """
    Parses a single Excel file. Detects XBRL block codes for classification.
    Tracks Start/End Date changes within the file to compute sub-periods
    (e.g. Q3 vs 9M) so both quarterly and cumulative data are preserved.

    Returns list of dicts: {key, label, value, text, section, subsection, sub_period}
    """
    df = _try_read_excel(file_path)
    if df is None:
        return []

    block_code_pattern = re.compile(r'\[(\d+)\]')

    try:
        data = []
        current_block_code = None
        current_start_date = None
        current_end_date = None
        current_sub_period = None  # None = use filename period

        for _, row in df.iterrows():
            label = str(row.iloc[0]).strip()
            raw_value = row.iloc[1] if len(row) > 1 else ''

            # Detect block code header rows
            block_match = block_code_pattern.match(label)
            if block_match:
                current_block_code = block_match.group(1)
                continue

            if not label or len(label) < 2:
                continue
            key = clean_key(label)
            if not key:
                continue

            # --- Track date changes within the file ---
            if key in _DATE_START_KEYS:
                date_val = _extract_date_value(raw_value)
                if date_val:
                    current_start_date = date_val
                    # Recompute sub_period when we have both dates
                    if current_end_date:
                        computed = _compute_period_from_dates(current_start_date, current_end_date)
                        if computed:
                            current_sub_period = computed

            if key in _DATE_END_KEYS:
                date_val = _extract_date_value(raw_value)
                if date_val:
                    current_end_date = date_val
                    # Recompute sub_period when we have both dates
                    if current_start_date:
                        computed = _compute_period_from_dates(current_start_date, current_end_date)
                        if computed:
                            current_sub_period = computed

            # --- Parse value ---
            val_num = None
            val_text = None

            if pd.notna(raw_value) and raw_value != '':
                try:
                    clean_str = str(raw_value).replace(',', '').strip()
                    if clean_str and clean_str.replace('.', '', 1).replace('-', '', 1).isdigit():
                        val_num = float(clean_str)
                    else:
                        val_text = str(raw_value).strip()
                except Exception:
                    val_text = str(raw_value).strip()

            clean_label = label[:500].replace('\t', ' ').replace('\n', ' ')
            if val_text:
                val_text = val_text.replace('\t', ' ').replace('\n', ' ')

            section, subsection = classify_metric(key, clean_label, block_code=current_block_code)

            data.append({
                "key": key,
                "label": clean_label,
                "value": val_num,
                "text": val_text,
                "section": section,
                "subsection": subsection,
                "sub_period": current_sub_period,
            })

        return data
    except Exception as e:
        print(f"    ❌ Error parsing rows in {os.path.basename(file_path)}: {e}")
        return []


# ---------------------------------------------------------------------------
# Per-symbol extraction (run in thread pool)
# ---------------------------------------------------------------------------

def extract_symbol_data(symbol, base_dir):
    """Extract all XBRL metrics for a single symbol. Thread-safe."""
    symbol_dir = os.path.join(base_dir, str(symbol))
    if not os.path.exists(symbol_dir):
        return [], None

    files = [f for f in os.listdir(symbol_dir)
             if 'XBRL' in f and (f.endswith('.xls') or f.endswith('.xlsx'))]

    symbol_metrics = []
    failed_files = []

    for filename in files:
        try:
            parts = os.path.splitext(filename)[0].split('_')
            year = int(parts[0])
            fallback_period = parts[-1]

            metrics = parse_excel_file(os.path.join(symbol_dir, filename))
            if metrics:
                for m in metrics:
                    # Use the sub_period computed from dates inside the file,
                    # or fall back to the period from the filename.
                    effective_period = m.get("sub_period") or fallback_period
                    symbol_metrics.append((
                        str(symbol), year, effective_period,
                        m["key"], m["value"], m["text"], m["label"],
                        filename, m["section"], m["subsection"],
                    ))
            else:
                failed_files.append(f"{symbol}: {filename}")
        except Exception as e:
            failed_files.append(f"{symbol}: {filename} ({str(e)[:50]})")

    return symbol_metrics, failed_files


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def clean_text_for_db(text):
    """Ensure text is valid UTF-8 and remove control characters."""
    if not isinstance(text, str):
        return text
    text = text.replace('\0', '').replace('\r', '').replace('\x00', '')
    try:
        return text.encode('utf-8', 'replace').decode('utf-8')
    except Exception:
        return ""


def ensure_metric_categories_exist(all_records):
    """
    Bulk-create any missing metric categories in one pass.
    Much faster than querying display_order per category.
    """
    print("📋 Ensuring metric categories exist in database...")
    session = Session()
    try:
        # Collect unique metrics from records
        unique_metrics = {}
        for rec in all_records:
            # rec: (symbol, year, period, key, value, text, label, file, section, subsection)
            metric_key = rec[3]
            if metric_key not in unique_metrics:
                unique_metrics[metric_key] = {
                    'section': rec[8],
                    'subsection': rec[9],
                    'label': rec[6],
                }

        # Query ALL existing keys in one go
        existing_keys = set(
            k[0] for k in session.query(FinancialMetricCategory.metric_name).all()
        )

        # Determine max display_order per section in one query
        from sqlalchemy import func as sa_func
        section_max_orders = dict(
            session.query(
                FinancialMetricCategory.section,
                sa_func.max(FinancialMetricCategory.display_order),
            ).group_by(FinancialMetricCategory.section).all()
        )

        new_categories = []
        for metric_key, info in unique_metrics.items():
            if metric_key in existing_keys:
                continue
            section = info['section']
            current_max = section_max_orders.get(section, -1) or -1
            next_order = current_max + 1
            section_max_orders[section] = next_order  # bump for next metric in same section

            new_categories.append(FinancialMetricCategory(
                metric_name=metric_key,
                section=section,
                subsection=info['subsection'],
                description_en=info['label'],
                unit='SAR',
                display_order=next_order,
                is_key_metric=False,
                is_calculated=False,
            ))

        if new_categories:
            session.bulk_save_objects(new_categories)
            session.commit()
            print(f"   ✅ Created {len(new_categories)} new metric categories")
        else:
            print(f"   ℹ️  All {len(unique_metrics)} metrics already exist")

        return True
    except Exception as e:
        print(f"   ❌ Error creating categories: {e}")
        session.rollback()
        return False
    finally:
        session.close()


def save_to_db_fast(all_records):
    """
    Saves metrics using DELETE + copy_expert (CSV) INSERT in chunks.
    Deletes existing data for processed symbols first, then bulk-inserts fresh data.
    No unique constraint required.
    """
    if not all_records:
        print("⚠️ No records to save.")
        return 0

    # --- Ensure categories exist (bulk) ---
    ensure_metric_categories_exist(all_records)

    # --- Dedup in Python (keep last occurrence per unique key) ---
    print("🧹 Deduplicating...")
    unique_map = {}
    for rec in all_records:
        unique_map[(rec[0], rec[1], rec[2], rec[3])] = rec
    dedup = list(unique_map.values())
    removed = len(all_records) - len(dedup)
    if removed:
        print(f"   Removed {removed:,} duplicates.")

    total_records = len(dedup)

    # --- Delete old data for the symbols we're about to insert ---
    symbols_in_batch = list(set(rec[0] for rec in dedup))
    print(f"🗑️  Clearing old data for {len(symbols_in_batch)} symbols...")

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        # Use ANY(array) for efficient multi-symbol delete
        cursor.execute(
            "DELETE FROM company_financial_metrics WHERE company_symbol = ANY(%s)",
            (symbols_in_batch,)
        )
        deleted = cursor.rowcount
        conn.commit()
        cursor.close()
        if deleted:
            print(f"   Deleted {deleted:,} old records.")
    except Exception as e:
        print(f"   ⚠️ Delete warning: {e}")
        try:
            conn.rollback()
        except Exception:
            pass

    # --- Bulk INSERT ---
    print(f"💾 Inserting {total_records:,} records...")

    CHUNK_SIZE = 10_000
    total_saved = 0

    try:
        for i in range(0, total_records, CHUNK_SIZE):
            chunk = dedup[i : i + CHUNK_SIZE]
            chunk_num = (i // CHUNK_SIZE) + 1
            total_chunks = ((total_records - 1) // CHUNK_SIZE) + 1

            if chunk_num % 5 == 0 or chunk_num == 1 or chunk_num == total_chunks:
                print(f"   ↳ Chunk {chunk_num}/{total_chunks} ({len(chunk):,} records)")

            cursor = conn.cursor()

            # Create temp table (dropped on commit)
            cursor.execute("""
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

            # Build CSV buffer (only first 8 fields — skip section/subsection)
            csv_buf = io.StringIO()
            for row in chunk:
                parts = []
                for idx in range(8):
                    item = row[idx]
                    if idx == 4:  # metric_value (DOUBLE PRECISION)
                        if item is None or str(item).strip() == '':
                            parts.append('')
                        else:
                            try:
                                parts.append(str(float(item)))
                            except (ValueError, TypeError):
                                parts.append('')
                    else:  # text columns
                        if item is None or str(item).strip() == '':
                            parts.append('')
                        else:
                            val = clean_text_for_db(str(item).strip())
                            val = val.replace('\\', '\\\\').replace('"', '""')
                            if any(c in val for c in [',', '"', '\n']):
                                val = f'"{val}"'
                            parts.append(val)
                csv_buf.write(','.join(parts) + '\n')

            csv_buf.seek(0)

            # COPY into temp table
            try:
                cursor.copy_expert(
                    "COPY tmp_metrics FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL '')",
                    csv_buf,
                )
            except Exception as e:
                print(f"   ❌ COPY Error (chunk {chunk_num}): {str(e)[:120]}")
                conn.rollback()
                cursor.close()
                continue

            # INSERT into real table (no conflict handling needed — we already deleted)
            cursor.execute("""
                INSERT INTO company_financial_metrics
                    (company_symbol, year, period, metric_name, metric_value, metric_text, label_en, source_file)
                SELECT company_symbol, year, period, metric_name, metric_value, metric_text, label_en, source_file
                FROM tmp_metrics;
            """)

            conn.commit()
            cursor.close()
            total_saved += len(chunk)

        print(f"✅ Successfully saved {total_saved:,} records.")
        return total_saved

    except Exception as e:
        print(f"❌ Database Error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return total_saved
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Enhanced Batch XBRL Extraction with Categorization")
    parser.add_argument('--workers', type=int, default=8,
                        help="Number of parallel workers for file parsing")
    parser.add_argument('--symbols', nargs='+',
                        help="Specific symbols to process (e.g. 1010 2222 4001)")
    args = parser.parse_args()

    base_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "downloads",
    )

    if not os.path.exists(base_dir):
        print(f"❌ Directory not found: {base_dir}")
        return

    all_symbols = sorted(
        d for d in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, d))
    )

    if args.symbols:
        # Support both: --symbols 1010 2222  AND  --symbols 6012,2001,8230
        expanded = []
        for s in args.symbols:
            expanded.extend(s.split(','))
        requested = [x.strip() for x in expanded if x.strip()]
        all_symbols = [s for s in all_symbols if s in requested]

    if not all_symbols:
        print("❌ No symbols found.")
        return

    print(f"{'=' * 60}")
    print(f"🚀 BATCH XBRL EXTRACTION (ENHANCED)")
    print(f"📊 Symbols: {len(all_symbols)}")
    print(f"⚡ Workers: {args.workers}")
    print(f"{'=' * 60}")

    start_time = time.time()
    all_data = []
    failed_symbols = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(extract_symbol_data, sym, base_dir): sym
            for sym in all_symbols
        }
        completed = 0
        for future in as_completed(futures):
            res, failed = future.result()
            if res:
                all_data.extend(res)
            if failed:
                failed_symbols.extend(failed)
            completed += 1
            if completed % 20 == 0 or completed == len(all_symbols):
                print(f"   📂 Parsed {completed}/{len(all_symbols)} symbols...")

    print(f"\n✅ Extraction: {len(all_data):,} metrics from {len(all_symbols)} symbols.")

    if failed_symbols:
        print(f"\n⚠️  Failed to read {len(failed_symbols)} files:")
        for f in failed_symbols[:20]:
            print(f"   - {f}")
        if len(failed_symbols) > 20:
            print(f"   ... and {len(failed_symbols) - 20} more")

    save_to_db_fast(all_data)

    elapsed = time.time() - start_time
    print(f"\n🏁 Finished in {elapsed:.1f}s ({elapsed / max(len(all_symbols), 1):.1f}s per symbol)")


if __name__ == "__main__":
    main()