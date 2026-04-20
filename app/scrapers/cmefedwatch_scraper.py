"""
CME FedWatch Tool Scraper - نسخة مصلّحة للـ Parser
====================================================
المشكلة: جدول QuikStrike بيجي في شكلين:
  1. خلايا مدمجة بـ \\n و \\t في نفس الـ row (table[1])
  2. جداول منفصلة بـ header عادي (table[2,3,4])

الـ parser القديم كان بيفشل في شكل 1 لأنه:
  - بيعتبر row[0] header لأنه فيه "meeting date" كجزء من النص المدمج
  - الـ rows التالية مش بيانات حقيقية فبيرجع قائمة فاضية

الحل: _try_parse_merged_cells() تحلّل النص المدمج مباشرة
      + تنظيف الـ headers من \\n و \\t في كل الأشكال
"""

import time
import json
import logging
from datetime import date, datetime

logger = logging.getLogger(__name__)

QUIKSTRIKE_DIRECT_URL = (
    "https://cmegroup-tools.quikstrike.net/User/QuikStrikeView.aspx"
    "?viewitemid=IntegratedFedWatchTool"
    "&insid=218288033"
    "&qsid=97a804a0-4076-406c-ade5-7148f4e9dafa"
)
CME_MAIN_URL = (
    "https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html"
)


# ─── حفظ في DB ────────────────────────────────────────────────────────────────
def _save_records(records: list[dict], today: date) -> bool:
    if not records:
        logger.warning("⚠️ لا توجد سجلات للحفظ")
        return False

    from app.core.database import SessionLocal
    from app.models.economic_indicators import CmeFedwatch

    db = SessionLocal()
    try:
        ins = upd = 0
        for rec in records:
            existing = (
                db.query(CmeFedwatch)
                .filter(
                    CmeFedwatch.scrape_date  == rec["scrape_date"],
                    CmeFedwatch.meeting_date == rec["meeting_date"],
                    CmeFedwatch.rate_range   == rec["rate_range"],
                )
                .first()
            )
            if existing:
                existing.probability = rec["probability"]
                upd += 1
            else:
                db.add(CmeFedwatch(**rec))
                ins += 1
        db.commit()
        logger.info(f"💾 DB: {ins} inserted, {upd} updated (date: {today})")
        return True
    except Exception as exc:
        db.rollback()
        logger.error(f"❌ DB error: {exc}")
        return False
    finally:
        db.close()


# ─── بناء الـ Driver ───────────────────────────────────────────────────────────
def _build_driver():
    import undetected_chromedriver as uc

    opts = uc.ChromeOptions()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-software-rasterizer")

    driver = uc.Chrome(options=opts, headless=False, use_subprocess=True)
    driver.set_page_load_timeout(120)
    driver.set_script_timeout(60)
    return driver


# ─── الدالة الرئيسية ──────────────────────────────────────────────────────────
def scrape_cme_fedwatch() -> bool:
    logger.info("🚀 CME FedWatch scraper بيبدأ...")
    today = date.today()

    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except ImportError:
        logger.error("❌ selenium غير مثبّت")
        return False

    driver = _build_driver()

    try:
        # ── الخطوة 1: فتح صفحة CME FedWatch ─────────────────────────────────
        logger.info("🌐 فتح صفحة CME FedWatch...")
        driver.get(CME_MAIN_URL)

        try:
            WebDriverWait(driver, 25).until(
                EC.presence_of_element_located(("tag name", "body"))
            )
        except Exception:
            pass

        time.sleep(15)

        page_title = driver.title
        current_url = driver.current_url
        logger.info(f"📄 Page: '{page_title}' @ {current_url[:60]}")

        # ── الخطوة 2: دوّر على iframe QuikStrike ────────────────────────────
        iframe_found = False

        try:
            iframe_info = driver.execute_script("""
                var iframes = document.querySelectorAll('iframe');
                var result = [];
                for (var i = 0; i < iframes.length; i++) {
                    result.push({
                        idx: i,
                        src: iframes[i].src || '',
                        id: iframes[i].id || '',
                        cls: iframes[i].className || '',
                        w: iframes[i].offsetWidth,
                        h: iframes[i].offsetHeight
                    });
                }
                return JSON.stringify(result);
            """)
            import json as json_mod
            frames = json_mod.loads(iframe_info)
            logger.info(f"🔍 {len(frames)} iframe(s) found:")
            for f in frames:
                logger.info(f"   [{f['idx']}] src={f['src'][:80]} id={f['id']} size={f['w']}x{f['h']}")

            for f in frames:
                src = f['src'].lower()
                if f['w'] < 100 or f['h'] < 100:
                    continue
                if 'quikstrike' in src:
                    all_iframes = driver.find_elements(By.TAG_NAME, "iframe")
                    if f['idx'] < len(all_iframes):
                        driver.switch_to.frame(all_iframes[f['idx']])
                        iframe_found = True
                        logger.info(f"✅ دخلت iframe [{f['idx']}] بنجاح!")
                        break

            if not iframe_found and frames:
                largest = max(frames, key=lambda x: x['w'] * x['h'])
                if largest['w'] > 100 and largest['h'] > 100:
                    all_iframes = driver.find_elements(By.TAG_NAME, "iframe")
                    if largest['idx'] < len(all_iframes):
                        driver.switch_to.frame(all_iframes[largest['idx']])
                        iframe_found = True
                        logger.info(f"✅ دخلت أكبر iframe [{largest['idx']}] ({largest['w']}x{largest['h']})")

        except Exception as e:
            logger.warning(f"⚠️ JS iframe scan error: {e}")

        if not iframe_found:
            logger.warning("⚠️ مفيش iframe — جرب QuikStrike مباشرة...")
            driver.get(QUIKSTRIKE_DIRECT_URL)
            time.sleep(5)

        # ── الخطوة 3: انتظر الجدول الفعلي ────────────────────────────────────
        logger.info("⏳ انتظار جدول FedWatch...")
        table_loaded = False
        try:
            WebDriverWait(driver, 40).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "table.grid-thm")
                )
            )
            table_loaded = True
            logger.info("✅ جدول grid-thm تحمّل")
        except Exception:
            logger.warning("⚠️ grid-thm لم يظهر — سأجرب tbody")
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "tbody"))
                )
                table_loaded = True
                logger.info("✅ tbody تحمّل")
            except Exception:
                logger.warning("⚠️ لم يظهر أي جدول — سأحاول التحليل")

        time.sleep(2)

        # ── الخطوة 4: انقر على Probabilities ────────────────────────────────
        _try_click(driver, "Probabilities")

        # ── الخطوة 5: استخرج البيانات ────────────────────────────────────────
        records = _extract(driver, today)

        if not records:
            logger.error(
                f"❌ فشل الاستخراج.\n"
                f"   Title: {driver.title}\n"
                f"   table_loaded: {table_loaded}\n"
                f"   DOM (2000 حرف): {driver.page_source[:2000]}"
            )
            return False

        logger.info(f"✅ {len(records)} سجل جاهز للحفظ")
        return _save_records(records, today)

    except Exception as exc:
        logger.error(f"❌ خطأ عام: {exc}", exc_info=True)
        return False
    finally:
        try:
            driver.quit()
        except Exception:
            pass


# ─── النقر على تبويب ──────────────────────────────────────────────────────────
def _try_click(driver, text: str) -> bool:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    # الطريقة الأقوى: JavaScript للبحث عن النص في التبويبات العلوية حصراً
    try:
        success = driver.execute_script(f"""
            // نبحث في الروابط اللي جوه قوائم أو تابات
            var els = document.querySelectorAll('.nav a, .tabs a, ul li a, button, span.tab-text');
            if (els.length === 0) els = document.querySelectorAll('a, span, li, button, div');
            
            for (var i = 0; i < els.length; i++) {{
                var t = els[i].innerText || els[i].textContent || '';
                if (t.trim().toLowerCase() === '{text.lower()}') {{
                    els[i].click();
                    return true;
                }}
            }}
            return false;
        """)
        if success:
            logger.info(f"✅ نقرت على '{text}' بنجاح باستخدام JS")
            time.sleep(6)  # زودنا الانتظار لأن الجدول الكبير بياخد وقت أطول ليحمل 
            return True
    except Exception as e:
        logger.warning(f"⚠️ JS click error: {e}")

    # Fallback للطريقة التقليدية
    for by, sel in [
        (By.LINK_TEXT,         text),
        (By.PARTIAL_LINK_TEXT, text[:6]),
    ]:
        try:
            el = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((by, sel))
            )
            el.click()
            logger.info(f"✅ نقرت على '{text}'")
            time.sleep(6)
            return True
        except Exception:
            pass

    logger.warning(f"⚠️ لم أجد '{text}'")
    return False


# ─── استخراج البيانات بـ JavaScript ──────────────────────────────────────────
def _extract(driver, today: date) -> list[dict]:
    js = r"""
    var out = {tables: []};
    document.querySelectorAll('table').forEach(function(tbl, ti) {
        var tdata = {index: ti, cls: tbl.className, rows: []};
        tbl.querySelectorAll('tr').forEach(function(tr) {
            var cells = [];
            tr.querySelectorAll('td,th').forEach(function(td) {
                var txt = (td.innerText || td.textContent || '').trim();
                cells.push(txt);
            });
            if (cells.some(function(c){ return c !== ''; }))
                tdata.rows.push(cells);
        });
        if (tdata.rows.length) out.tables.push(tdata);
    });
    return JSON.stringify(out);
    """
    try:
        data = json.loads(driver.execute_script(js))
    except Exception as e:
        logger.error(f"❌ JS error: {e}")
        return []

    logger.info(f"📋 عدد الجداول: {len(data['tables'])}")
    for tbl in data["tables"]:
        logger.info(
            f"   table[{tbl['index']}] cls='{tbl['cls']}' "
            f"({len(tbl['rows'])} rows) → {tbl['rows'][:2]}"
        )

    records = []
    for tbl in data["tables"]:
        parsed = _parse_fedwatch_table(tbl["rows"], today)
        if parsed:
            records.extend(parsed)
            logger.info(f"   ✅ table[{tbl['index']}] → {len(parsed)} سجل")

    return records


# ─── تحليل الجدول (مُصلَح) ────────────────────────────────────────────────────
def _parse_fedwatch_table(rows: list[list[str]], today: date) -> list[dict]:
    """
    يتعامل مع 3 أشكال للجدول:

    شكل 1 — نص مدمج في خلايا (table[1] في الـ logs):
        خلية تحتوي: "29 Apr 2026\\tZQJ6\\t..."
        خلية أخرى:  "0.0 %\\t99.5 %\\t0.5 %"
        → تُحلَّل بـ _try_parse_merged_cells()

    شكل 2 — Rate Ranges (الأعمدة = نطاقات سعرية "NNN-NNN"):
        ['MEETING DATE', '200-225', '225-250', ..., '375-400']
        ['4/29/2026',   '0.0%',    '0.0%',   ..., '97.9%'  ]

    شكل 3 — Aggregated (الأعمدة = EASE / NO CHANGE / HIKE):
        ['MEETING DATE', 'EASE', 'NO CHANGE', 'HIKE']
        ['4/29/2026',    '0.0%', '97.9%',    '2.1%']
    """
    if not rows:
        return []

    # ══ أول: جرب الـ header العادي (Rate Ranges أو Aggregated) ══════════════
    # مهم: لازم نجرب ده الأول عشان الـ merged cells parser ممكن يلتقط القيم الغلط
    header_idx = None
    for i, row in enumerate(rows):
        flat = [c.split("\n")[0].split("\t")[0].strip().lower() for c in row]
        if any("meeting date" in c for c in flat):
            header_idx = i
            break

    if header_idx is not None:
        header = [c.split("\n")[0].split("\t")[0].strip() for c in rows[header_idx]]
        logger.info(f"   Header (row {header_idx}): {header}")
        records = []

        # ── شكل 1: Rate Ranges ──────────────────────────────────────────────
        rate_cols = []
        for ci, col in enumerate(header):
            if ci == 0:
                continue
            parts = col.strip().split("-")
            if len(parts) == 2 and all(p.strip().isdigit() for p in parts):
                rate_cols.append((ci, col.strip()))

        if rate_cols:
            logger.info(f"   → شكل 1 (Rate Ranges): {[r for _, r in rate_cols]}")
            for row in rows[header_idx + 1:]:
                if not row or not row[0].strip():
                    continue
                raw_date = row[0].split("\n")[0].split("\t")[0].strip()
                meeting_date = _parse_date(raw_date)
                if not meeting_date:
                    continue
                meeting_date_str = meeting_date.strftime("%Y-%m-%d")
                for ci, rate_range in rate_cols:
                    if ci >= len(row):
                        continue
                    prob = _to_float(row[ci])
                    if prob is None:
                        continue
                    records.append({
                        "scrape_date":  today,
                        "meeting_date": meeting_date_str,
                        "rate_range":   rate_range,
                        "probability":  prob,
                    })
            return records

        # ── شكل 2: Ease / No Change / Hike ──────────────────────────────────
        label_cols = []
        for ci, col in enumerate(header):
            cl = col.strip().lower()
            if "ease" in cl:
                label_cols.append((ci, "Ease"))
            elif "no change" in cl or "no_change" in cl:
                label_cols.append((ci, "No Change"))
            elif "hike" in cl:
                label_cols.append((ci, "Hike"))

        if label_cols:
            logger.info(f"   → شكل 2 (Aggregated): {label_cols}")
            for row in rows[header_idx + 1:]:
                if not row or not row[0].strip():
                    continue
                raw_date = row[0].split("\n")[0].split("\t")[0].strip()
                meeting_date = _parse_date(raw_date)
                if not meeting_date:
                    continue
                meeting_date_str = meeting_date.strftime("%Y-%m-%d")
                for ci, label in label_cols:
                    if ci >= len(row):
                        continue
                    prob = _to_float(row[ci])
                    if prob is None:
                        continue
                    records.append({
                        "scrape_date":  today,
                        "meeting_date": meeting_date_str,
                        "rate_range":   label,
                        "probability":  prob,
                    })
            return records

    # ══ Fallback: خلايا مدمجة (لما مفيش header صريح بـ "MEETING DATE") ════
    return _try_parse_merged_cells(rows, today)



# ─── شكل 1: خلايا مدمجة ──────────────────────────────────────────────────────
def _try_parse_merged_cells(rows: list[list[str]], today: date) -> list[dict]:
    """
    يحلّل الحالة اللي بيكون فيها table[1] — كل المعلومات في خلايا كبيرة مدمجة:

    مثال من الـ logs:
    row[0] = [
      "MEETING INFORMATION\\nMEETING DATE\\tCONTRACT\\t...\\n29 Apr 2026\\tZQJ6\\t...",
      "PROBABILITIES\\nEASE\\tNO CHANGE\\tHIKE\\n0.0 %\\t99.5 %\\t0.5 %",
      ...
    ]

    الخوارزمية:
    - نفك كل خلية إلى سطور وكلمات
    - نبحث عن تواريخ الاجتماعات
    - نبحث عن الاحتمالات (أرقام + %) بالترتيب: Ease, No Change, Hike
    - نربط كل تاريخ بالاحتمالات المجاورة له
    """
    records = []

    # ── نجمع كل النصوص في list مسطّحة مع تتبع الـ rows ──────────────────────
    # نبحث عن pattern: تاريخ ثم 3 احتمالات (أو 2) في نفس block
    for row in rows:
        meeting_date = None
        probabilities = []  # قائمة مرتّبة: [ease, no_change, hike]

        # ── نفك كل خلية ونجمع الـ tokens ────────────────────────────────────
        all_tokens = []
        for cell in row:
            if not cell:
                continue
            # استبدل \t بـ \n عشان نعامل الاثنين زي بعض
            normalized = cell.replace("\t", "\n")
            lines = [l.strip() for l in normalized.split("\n") if l.strip()]
            all_tokens.extend(lines)

        # ── دوّر على الـ tokens ───────────────────────────────────────────────
        i = 0
        while i < len(all_tokens):
            token = all_tokens[i]

            # تاريخ؟
            if meeting_date is None:
                d = _parse_date(token)
                if d:
                    meeting_date = d
                    i += 1
                    continue

            # احتمالية (رقم%)؟
            if "%" in token:
                val = _to_float(token)
                if val is not None and len(probabilities) < 3:
                    probabilities.append(val)

            i += 1

        # ── إذا وجدنا تاريخ واحتمالية واحدة على الأقل ── ────────────────────
        if meeting_date and probabilities:
            date_str = meeting_date.strftime("%Y-%m-%d")
            labels = ["Ease", "No Change", "Hike"]
            for label, val in zip(labels, probabilities):
                records.append({
                    "scrape_date":  today,
                    "meeting_date": date_str,
                    "rate_range":   label,
                    "probability":  val,
                })
            logger.info(
                f"   ✅ merged: {date_str} → "
                + ", ".join(f"{l}={v}" for l, v in zip(labels, probabilities))
            )

    return records


# ─── أدوات مساعدة ─────────────────────────────────────────────────────────────
def _parse_date(text: str) -> date | None:
    """يحاول تحليل النص كتاريخ بعدة صيغ شائعة."""
    clean = text.strip()
    # تخطى النصوص الطويلة جداً (مش تاريخ)
    if len(clean) > 20:
        return None
    for fmt in (
        "%m/%d/%Y",   # 4/29/2026
        "%d %b %Y",   # 29 Apr 2026
        "%b %d, %Y",  # Apr 29, 2026
        "%Y-%m-%d",   # 2026-04-29
        "%d/%m/%Y",   # 29/04/2026
        "%d %B %Y",   # 29 April 2026
    ):
        try:
            return datetime.strptime(clean, fmt).date()
        except ValueError:
            pass
    return None


def _to_float(text: str) -> float | None:
    """يحوّل النص لرقم عشري بعد إزالة % والمسافات."""
    clean = text.replace("%", "").strip()
    if not clean:
        return None
    try:
        return float(clean)
    except ValueError:
        return None


# ─── للتشغيل اليدوي ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    success = scrape_cme_fedwatch()
    print("\n" + ("✅ نجح" if success else "❌ فشل"))