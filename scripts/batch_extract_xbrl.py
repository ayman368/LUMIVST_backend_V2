"""
Batch XBRL Extraction to PostgreSQL (Enhanced with Categorization)
===================================================================
1. Extracts data from XBRL Excel files using pandas (with robust fallback).
2. Automatically categorizes metrics into sections (Income Statement, Cash Flow, Balance Sheet).
3. Creates/updates financial metric categories in the database.
4. Separates clean numeric values from text values.
5. Uses PostgreSQL COPY protocol for ultra-fast bulk insertion.
6. Handles duplicates and saves in small chunks to prevent buffer overflows.
"""

import os
import sys
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

# Mapping of keywords to sections and subsections
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

def clean_key(text):
    """Generates a clean snake_case key from the label."""
    if not isinstance(text, str): return f"unknown_{text}"
    text = text.split('[')[0].split('|')[0]
    clean = "".join(c if c.isalnum() else "_" for c in text)
    clean = clean.lower().strip("_")
    while "__" in clean: clean = clean.replace("__", "_")
    return clean[:100]

def classify_metric(metric_key, metric_label, block_code=None):
    """
    Classifies a metric into a section and subsection.
    First tries to use actual XBRL block code, then falls back to keyword matching.
    Returns (section, subsection) or ('other', None) if unknown.
    """
    # 1. Try block code classification (most accurate)
    if block_code:
        block_dict = {
            '100010': ('filing_information', None),
            '200100': ('auditors_report', None),
            '300100': ('balance_sheet', None),
            '300200': ('income_statement', None),
            '300300': ('other_comprehensive_income', None),
            '300400': ('cash_flow', None),
            '300500': ('changes_in_equity', None),
            '400100': ('notes_to_accounts', None),
        }
        
        if block_code in block_dict:
            return block_dict[block_code]
        
        # Try prefix matching for block codes
        for code_prefix in ['100', '200', '300', '400']:
            if block_code.startswith(code_prefix):
                prefix_dict = {
                    '100': ('filing_information', None),
                    '200': ('auditors_report', None),
                    '300': ('income_statement', None),  # Statements are under 300
                    '400': ('notes_to_accounts', None),
                }
                if code_prefix in prefix_dict:
                    return prefix_dict[code_prefix]
    
    # 2. Special handling for specific subsections within 300 codes
    if block_code and block_code.startswith('300'):
        subsection_map = {
            '300100': ('balance_sheet', 'statement_of_financial_position'),
            '300200': ('income_statement', 'statement_of_income'),
            '300300': ('income_statement', 'other_comprehensive_income'),
            '300400': ('cash_flow', 'statement_of_cash_flows'),
            '300500': ('changes_in_equity', 'statement_of_changes_in_equity'),
        }
        if block_code in subsection_map:
            return subsection_map[block_code]
    
    # 3. Fall back to keyword-based classification
    metric_lower = metric_key.lower()
    label_lower = metric_label.lower() if isinstance(metric_label, str) else ""
    
    # Check metric key against mapping
    for keyword, (section, subsection) in METRIC_SECTION_MAPPING.items():
        if keyword in metric_lower or keyword in label_lower:
            return section, subsection
    
    # Default classification based on keywords in label
    if any(word in label_lower for word in ['balance', 'asset', 'liability', 'equity', 'statement of financial position']):
        return 'balance_sheet', None
    elif any(word in label_lower for word in ['income', 'revenue', 'expense', 'profit', 'loss', 'statement of comprehensive income']):
        return 'income_statement', None
    elif any(word in label_lower for word in ['cash flow', 'operating', 'investing', 'financing', 'statement of cash']):
        return 'cash_flow', None
    
    return 'other', None

def parse_excel_file(file_path):
    """
    Parses a single Excel file with robust fallback for older/damaged formats.
    Returns a list of dicts: {key, label, value (numeric), text (string), section, subsection}
    
    Detects XBRL block codes like [100010], [200100], [300100], etc. and uses them for classification.
    """
    import re
    
    df = None
    fname = os.path.basename(file_path)
    
    # 1. Try standard read_excel (modern XLSX)
    try:
        df = pd.read_excel(file_path, header=None, engine='openpyxl').fillna('')
    except Exception as e:
        pass

    # 2. Try read_excel with xlrd (legacy XLS)
    if df is None:
        try:
            df = pd.read_excel(file_path, header=None, engine='xlrd').fillna('')
        except Exception as e:
            pass

    # 3. Try read_html (for HTML disguised as XLS)
    if df is None:
        try:
            dfs = pd.read_html(file_path)
            if dfs:
                df = dfs[0].fillna('')
        except Exception as e:
            pass

    # 4. Try as CSV with different encodings
    if df is None:
        for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
            try:
                df = pd.read_csv(file_path, header=None, sep='\t', encoding=encoding).fillna('')
                break
            except:
                pass

    # 5. Try as CSV with comma separator
    if df is None:
        for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
            try:
                df = pd.read_csv(file_path, header=None, sep=',', encoding=encoding).fillna('')
                break
            except:
                pass

    if df is None:
        print(f"    ❌ FAILED to read {fname} (All methods failed)")
        return []
        
    try:
        data = []
        current_block_code = None
        block_code_pattern = re.compile(r'\[(\d+)\]')
        
        for index, row in df.iterrows():
            label = str(row[0]).strip()
            raw_value = row[1] if len(row) > 1 else ''
            
            # Check if this row starts with a block code
            block_match = block_code_pattern.match(label)
            if block_match:
                current_block_code = block_match.group(1)
                # Skip the block header row itself, but track it for future rows
                continue
            
            if not label or len(label) < 2: continue
            key = clean_key(label)
            if not key: continue

            val_num = None
            val_text = None
            
            if pd.notna(raw_value) and raw_value != '':
                try:
                    clean_str = str(raw_value).replace(',', '').strip()
                    if clean_str and clean_str.replace('.', '', 1).replace('-', '', 1).isdigit():
                        val_num = float(clean_str)
                    else:
                        val_text = str(raw_value).strip()
                except:
                    val_text = str(raw_value).strip()
            
            clean_label = label[:500].replace('\t', ' ').replace('\n', ' ')
            if val_text: val_text = val_text.replace('\t', ' ').replace('\n', ' ')

            # Classify using block code (most accurate method)
            section, subsection = classify_metric(key, clean_label, block_code=current_block_code)

            data.append({
                "key": key,
                "label": clean_label,
                "value": val_num,
                "text": val_text,
                "section": section,
                "subsection": subsection
            })
            
        return data
    except Exception as e:
        print(f"    ❌ Error parsing rows in {fname}: {e}")
        return []

def extract_symbol_data(symbol, base_dir):
    symbol_dir = os.path.join(base_dir, str(symbol))
    if not os.path.exists(symbol_dir): return [], None
    
    files = [f for f in os.listdir(symbol_dir) if 'XBRL' in f and (f.endswith('.xls') or f.endswith('.xlsx'))]
    symbol_metrics = []
    failed_files = []
    
    for filename in files:
        try:
            parts = os.path.splitext(filename)[0].split('_')
            year = int(parts[0])
            period = parts[-1]
            
            metrics = parse_excel_file(os.path.join(symbol_dir, filename))
            if metrics:
                for m in metrics:
                    symbol_metrics.append((str(symbol), year, period, m["key"], m["value"], m["text"], m["label"], filename, m["section"], m["subsection"]))
            else:
                failed_files.append(f"{symbol}: {filename}")
        except Exception as e: 
            failed_files.append(f"{symbol}: {filename} ({str(e)[:50]})")
            continue
    
    return symbol_metrics, failed_files

def clean_text_for_db(text):
    """Ensure text is valid UTF-8 and remove control characters."""
    if not isinstance(text, str): return text
    text = text.replace('\0', '').replace('\r', '').replace('\x00', '')
    try:
        return text.encode('utf-8', 'replace').decode('utf-8')
    except:
        return ""

def ensure_metric_categories_exist(all_records):
    """
    Ensures that all metric categories exist in the database.
    Creates missing categories with auto-incremented display_order.
    """
    print("📋 Ensuring metric categories exist in database...")
    
    session = Session()
    try:
        # Collect unique metrics from records
        unique_metrics = {}
        for rec in all_records:
            # rec format: (symbol, year, period, metric_key, value, text, label, source_file, section, subsection)
            metric_key = rec[3]
            section = rec[8]
            subsection = rec[9]
            label = rec[6]
            
            if metric_key not in unique_metrics:
                unique_metrics[metric_key] = {
                    'section': section,
                    'subsection': subsection,
                    'label': label
                }
        
        # Query existing categories
        existing_keys = set(
            k[0] for k in session.query(FinancialMetricCategory.metric_name).all()
        )
        
        new_categories = []
        for idx, (metric_key, info) in enumerate(unique_metrics.items()):
            if metric_key not in existing_keys:
                # Get current max display_order
                max_order = session.query(FinancialMetricCategory).filter(
                    FinancialMetricCategory.section == info['section']
                ).order_by(FinancialMetricCategory.display_order.desc()).first()
                
                next_order = (max_order.display_order + 1) if max_order else 0
                
                category = FinancialMetricCategory(
                    metric_name=metric_key,
                    section=info['section'],
                    subsection=info['subsection'],
                    description_en=info['label'],
                    unit='SAR',
                    display_order=next_order,
                    is_key_metric=False,
                    is_calculated=False
                )
                new_categories.append(category)
        
        if new_categories:
            session.add_all(new_categories)
            session.commit()
            print(f"   ✅ Created {len(new_categories)} new metric categories")
        else:
            print(f"   ℹ️ All {len(unique_metrics)} metrics already exist")
        
        return True
    except Exception as e:
        print(f"   ❌ Error creating categories: {e}")
        session.rollback()
        return False
    finally:
        session.close()

def save_to_db_fast(all_records):
    """
    Saves metrics to DB using CHUNKED COPY to handle large datasets safely.
    Handles 'invalid input syntax for double precision' by ensuring numeric columns get \\N not "".
    """
    if not all_records:
        print("⚠️ No records to save.")
        return 0
    
    # Ensure categories exist first
    ensure_metric_categories_exist(all_records)
    
    # Keep all records including duplicates
    total_records = len(all_records)
    print(f"💾 Saving {total_records:,} records to Database (in chunks)...")
    
    # 1. Config: SMALL CHUNK SIZE to prevent buffer overflow
    CHUNK_SIZE = 5000 
    total_saved = 0
    
    conn = engine.raw_connection()
    try:
        for i in range(0, total_records, CHUNK_SIZE):
            # Refresh cursor per chunk
            cursor = conn.cursor()
            chunk = all_records[i : i + CHUNK_SIZE]
            
            if (i // CHUNK_SIZE) % 5 == 0:
                print(f"   ↳ Processing chunk {i//CHUNK_SIZE + 1}/{(total_records//CHUNK_SIZE)+1} ({len(chunk):,} records)...")
            
            # Prepare CSV
            output = io.StringIO()
            for row in chunk:
                line_parts = []
                # row indices: 0=sym, 1=yr, 2=pd, 3=metric, 4=VAL, 5=txt, 6=lbl, 7=file, 8=section, 9=subsection
                for idx, item in enumerate(row[:8]):  # Only first 8 columns for the old schema
                    # Special handling for metric_value column (index 4 - DOUBLE PRECISION)
                    if idx == 4:
                        # For numeric metric_value column
                        if item is None or (isinstance(item, str) and str(item).strip() == ''):
                            line_parts.append(r"\N")
                        else:
                            try:
                                # Try to parse as float to validate
                                float_val = float(item)
                                line_parts.append(str(float_val))
                            except (ValueError, TypeError):
                                # If can't parse as float, treat as NULL for this numeric column
                                line_parts.append(r"\N")
                    else:
                        # For text columns
                        if item is None:
                            line_parts.append(r"\N")
                        else:
                            val = str(item).strip()
                            if val == "":
                                line_parts.append(r"\N")
                            else:
                                val = clean_text_for_db(val)
                                val = val.replace('\t', ' ').replace('\n', ' ').replace('\\', '\\\\')
                                line_parts.append(val)
                            
                output.write("\t".join(line_parts) + "\n")
            output.seek(0)
            
            # TEMP TABLE
            cursor.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_metrics (company_symbol TEXT, year INT, period TEXT, metric_name TEXT, metric_value DOUBLE PRECISION, metric_text TEXT, label_en TEXT, source_file TEXT) ON COMMIT DELETE ROWS;")
            
            # COPY
            try:
                cursor.copy_from(
                    output, 'tmp_metrics', null='\\N', 
                    columns=('company_symbol', 'year', 'period', 'metric_name', 'metric_value', 'metric_text', 'label_en', 'source_file')
                )
            except Exception as copy_err:
                print(f"   ❌ COPY Error in chunk {i//CHUNK_SIZE + 1}: {copy_err}")
                conn.rollback()
                continue

            # INSERT (allowing duplicates)
            cursor.execute("""
                INSERT INTO company_financial_metrics 
                (company_symbol, year, period, metric_name, metric_value, metric_text, label_en, source_file)
                SELECT company_symbol, year, period, metric_name, metric_value, metric_text, label_en, source_file 
                FROM tmp_metrics;
            """)
            
            conn.commit()
            cursor.close()
            total_saved += len(chunk)
            time.sleep(0.05) # Yield
            
        print(f"✅ Successfully saved {total_saved:,} records.")
        return total_saved
        
    except Exception as e:
        print(f"❌ Database Error: {e}")
        try: conn.rollback()
        except: pass
        return total_saved
    finally:
        try: conn.close()
        except: pass

def main():
    parser = argparse.ArgumentParser(description="Enhanced Batch XBRL Extraction with Categorization")
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--symbols', nargs='+', help="Specific symbols (e.g. 1010)")
    args = parser.parse_args()

    base_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "downloads")
    
    # Get all subdirectories
    if os.path.exists(base_dir):
        all_symbols = sorted([d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))])
    else:
        print(f"❌ Directory not found: {base_dir}")
        return

    if args.symbols:
        all_symbols = [s for s in all_symbols if s in args.symbols]

    if not all_symbols:
        print("❌ No symbols found.")
        return
    
    print(f"{'='*60}")
    print(f"🚀 STARTING ENHANCED BATCH EXTRACTION")
    print(f"📊 Symbols: {len(all_symbols)}")
    print(f"⚡ Workers: {args.workers}")
    print(f"{'='*60}")
    
    start_time = time.time()
    all_data = []
    failed_symbols = []
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(extract_symbol_data, sym, base_dir): sym for sym in all_symbols}
        completed = 0
        for future in as_completed(futures):
            res, failed = future.result()
            if res:
                all_data.extend(res)
            if failed:
                failed_symbols.extend(failed)
            completed += 1
            if completed % 20 == 0:
                print(f"   Progress: {completed}/{len(all_symbols)} processed...")

    print(f"\n✅ Extraction: {len(all_data):,} metrics found.")
    
    if failed_symbols:
        print(f"\n⚠️  Failed to read {len(failed_symbols)} files:")
        for f in failed_symbols[:20]:  # Show first 20
            print(f"   - {f}")
        if len(failed_symbols) > 20:
            print(f"   ... and {len(failed_symbols) - 20} more")
    
    save_to_db_fast(all_data)
    
    print(f"\n🏁 Finished in {time.time() - start_time:.1f}s")

if __name__ == "__main__":
    main()

def clean_key(text):
    """Generates a clean snake_case key from the label."""
    if not isinstance(text, str): return f"unknown_{text}"
    text = text.split('[')[0].split('|')[0]
    clean = "".join(c if c.isalnum() else "_" for c in text)
    clean = clean.lower().strip("_")
    while "__" in clean: clean = clean.replace("__", "_")
    return clean[:100]

def parse_excel_file(file_path):
    """
    Parses a single Excel file with robust fallback for older/damaged formats.
    Returns a list of dicts: {key, label, value (numeric), text (string)}
    """
    df = None
    fname = os.path.basename(file_path)
    
    # 1. Try standard read_excel (modern XLSX)
    try:
        df = pd.read_excel(file_path, header=None, engine='openpyxl').fillna('')
    except Exception as e:
        pass

    # 2. Try read_excel with xlrd (legacy XLS)
    if df is None:
        try:
            df = pd.read_excel(file_path, header=None, engine='xlrd').fillna('')
        except Exception as e:
            pass

    # 3. Try read_html (for HTML disguised as XLS)
    if df is None:
        try:
            dfs = pd.read_html(file_path)
            if dfs:
                df = dfs[0].fillna('')
        except Exception as e:
            pass

    # 4. Try as CSV with different encodings
    if df is None:
        for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
            try:
                df = pd.read_csv(file_path, header=None, sep='\t', encoding=encoding).fillna('')
                break
            except:
                pass

    # 5. Try as CSV with comma separator
    if df is None:
        for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
            try:
                df = pd.read_csv(file_path, header=None, sep=',', encoding=encoding).fillna('')
                break
            except:
                pass

    if df is None:
        print(f"    ❌ FAILED to read {fname} (All methods failed)")
        return []
        
    try:
        data = []
        for index, row in df.iterrows():
            label = str(row[0]).strip()
            raw_value = row[1] if len(row) > 1 else ''
            
            if not label or len(label) < 2: continue
            key = clean_key(label)
            if not key: continue

            val_num = None
            val_text = None
            
            if pd.notna(raw_value) and raw_value != '':
                try:
                    clean_str = str(raw_value).replace(',', '').strip()
                    if clean_str and clean_str.replace('.', '', 1).replace('-', '', 1).isdigit():
                        val_num = float(clean_str)
                    else:
                        val_text = str(raw_value).strip()
                except:
                    val_text = str(raw_value).strip()
            
            clean_label = label[:500].replace('\t', ' ').replace('\n', ' ')
            if val_text: val_text = val_text.replace('\t', ' ').replace('\n', ' ')

            data.append({
                "key": key,
                "label": clean_label,
                "value": val_num,
                "text": val_text
            })
            
        return data
    except Exception as e:
        print(f"    ❌ Error parsing rows in {fname}: {e}")
        return []

def extract_symbol_data(symbol, base_dir):
    symbol_dir = os.path.join(base_dir, str(symbol))
    if not os.path.exists(symbol_dir): return [], None
    
    files = [f for f in os.listdir(symbol_dir) if 'XBRL' in f and (f.endswith('.xls') or f.endswith('.xlsx'))]
    symbol_metrics = []
    failed_files = []
    
    for filename in files:
        try:
            parts = os.path.splitext(filename)[0].split('_')
            year = int(parts[0])
            period = parts[-1]
            
            metrics = parse_excel_file(os.path.join(symbol_dir, filename))
            if metrics:
                for m in metrics:
                    symbol_metrics.append((str(symbol), year, period, m["key"], m["value"], m["text"], m["label"], filename))
            else:
                failed_files.append(f"{symbol}: {filename}")
        except Exception as e: 
            failed_files.append(f"{symbol}: {filename} ({str(e)[:50]})")
            continue
    
    return symbol_metrics, failed_files

def clean_text_for_db(text):
    """Ensure text is valid UTF-8 and remove control characters."""
    if not isinstance(text, str): return text
    text = text.replace('\0', '').replace('\r', '').replace('\x00', '')
    try:
        return text.encode('utf-8', 'replace').decode('utf-8')
    except:
        return ""

def save_to_db_fast(all_records):
    """
    Saves metrics to DB using CHUNKED COPY to handle large datasets safely.
    Handles 'invalid input syntax for double precision' by ensuring numeric columns get \\N not "".
    """
    if not all_records:
        print("⚠️ No records to save.")
        return 0
    
    # Keep all records including duplicates
    total_records = len(all_records)
    print(f"💾 Saving {total_records:,} records to Database (in chunks)...")
    
    # 1. Config: SMALL CHUNK SIZE to prevent buffer overflow
    CHUNK_SIZE = 5000 
    total_saved = 0
    
    conn = engine.raw_connection()
    try:
        for i in range(0, total_records, CHUNK_SIZE):
            # Refresh cursor per chunk
            cursor = conn.cursor()
            chunk = all_records[i : i + CHUNK_SIZE]
            
            if (i // CHUNK_SIZE) % 5 == 0:
                print(f"   ↳ Processing chunk {i//CHUNK_SIZE + 1}/{(total_records//CHUNK_SIZE)+1} ({len(chunk):,} records)...")
            
            # Prepare CSV
            output = io.StringIO()
            for row in chunk:
                line_parts = []
                # row indices: 0=sym, 1=yr, 2=pd, 3=metric, 4=VAL, 5=txt, 6=lbl, 7=file
                for idx, item in enumerate(row):
                    # Special handling for metric_value column (index 4 - DOUBLE PRECISION)
                    if idx == 4:
                        # For numeric metric_value column
                        if item is None or (isinstance(item, str) and str(item).strip() == ''):
                            line_parts.append(r"\N")
                        else:
                            try:
                                # Try to parse as float to validate
                                float_val = float(item)
                                line_parts.append(str(float_val))
                            except (ValueError, TypeError):
                                # If can't parse as float, treat as NULL for this numeric column
                                line_parts.append(r"\N")
                    else:
                        # For text columns
                        if item is None:
                            line_parts.append(r"\N")
                        else:
                            val = str(item).strip()
                            if val == "":
                                line_parts.append(r"\N")
                            else:
                                val = clean_text_for_db(val)
                                val = val.replace('\t', ' ').replace('\n', ' ').replace('\\', '\\\\')
                                line_parts.append(val)
                            
                output.write("\t".join(line_parts) + "\n")
            output.seek(0)
            
            # TEMP TABLE
            cursor.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_metrics (company_symbol TEXT, year INT, period TEXT, metric_name TEXT, metric_value DOUBLE PRECISION, metric_text TEXT, label_en TEXT, source_file TEXT) ON COMMIT DELETE ROWS;")
            
            # COPY
            try:
                cursor.copy_from(
                    output, 'tmp_metrics', null='\\N', 
                    columns=('company_symbol', 'year', 'period', 'metric_name', 'metric_value', 'metric_text', 'label_en', 'source_file')
                )
            except Exception as copy_err:
                print(f"   ❌ COPY Error in chunk {i//CHUNK_SIZE + 1}: {copy_err}")
                conn.rollback()
                continue

            # INSERT (allowing duplicates)
            cursor.execute("""
                INSERT INTO company_financial_metrics 
                (company_symbol, year, period, metric_name, metric_value, metric_text, label_en, source_file)
                SELECT company_symbol, year, period, metric_name, metric_value, metric_text, label_en, source_file 
                FROM tmp_metrics;
            """)
            
            conn.commit()
            cursor.close()
            total_saved += len(chunk)
            time.sleep(0.05) # Yield
            
        print(f"✅ Successfully saved {total_saved:,} records.")
        return total_saved
        
    except Exception as e:
        print(f"❌ Database Error: {e}")
        try: conn.rollback()
        except: pass
        return total_saved
    finally:
        try: conn.close()
        except: pass

def main():
    parser = argparse.ArgumentParser(description="Clean Batch XBRL Extraction")
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--symbols', nargs='+', help="Specific symbols (e.g. 1010)")
    args = parser.parse_args()

    base_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "downloads")
    
    # Get all subdirectories
    if os.path.exists(base_dir):
        all_symbols = sorted([d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))])
    else:
        print(f"❌ Directory not found: {base_dir}")
        return

    if args.symbols:
        all_symbols = [s for s in all_symbols if s in args.symbols]

    if not all_symbols:
        print("❌ No symbols found.")
        return
    
    print(f"{'='*60}")
    print(f"🚀 STARTING BATCH EXTRACTION")
    print(f"📊 Symbols: {len(all_symbols)}")
    print(f"⚡ Workers: {args.workers}")
    print(f"{'='*60}")
    
    start_time = time.time()
    all_data = []
    failed_symbols = []
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(extract_symbol_data, sym, base_dir): sym for sym in all_symbols}
        completed = 0
        for future in as_completed(futures):
            res, failed = future.result()
            if res:
                all_data.extend(res)
            if failed:
                failed_symbols.extend(failed)
            completed += 1
            if completed % 20 == 0:
                print(f"   Progress: {completed}/{len(all_symbols)} processed...")

    print(f"\n✅ Extraction: {len(all_data):,} metrics found.")
    
    if failed_symbols:
        print(f"\n⚠️  Failed to read {len(failed_symbols)} files:")
        for f in failed_symbols[:20]:  # Show first 20
            print(f"   - {f}")
        if len(failed_symbols) > 20:
            print(f"   ... and {len(failed_symbols) - 20} more")
    
    save_to_db_fast(all_data)
    
    print(f"\n🏁 Finished in {time.time() - start_time:.1f}s")

if __name__ == "__main__":
    main()