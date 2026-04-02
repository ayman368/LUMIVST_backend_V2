import sys
from pathlib import Path
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import logging
import requests

# إضافة المسار للمجلد الرئيسي للوصول للإعدادات
sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.config import settings

# إعداد الـ Logging لمتابعة سير العملية
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TechnicalCalculator:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self._ensure_columns_exist()

    def _ensure_columns_exist(self):
        """تأكد من وجود الأعمدة الجديدة في stock_indicators (الحصرية لجدول prices فقط)"""
        logger.info("🔍 التحقق من وجود الأعمدة في stock_indicators...")

        # ملاحظة: تم نقل هذه الأعمدة من prices إلى stock_indicators
        columns_to_add = {
            'sma_10':  'NUMERIC(14, 4)',
            'sma_21':  'NUMERIC(14, 4)',
            'sma_50':  'NUMERIC(14, 4)',
            'sma_150': 'NUMERIC(14, 4)',
            'sma_200': 'NUMERIC(14, 4)',
            'sma_200_1m_ago': 'NUMERIC(14, 4)',
            'sma_200_2m_ago': 'NUMERIC(14, 4)',
            'sma_200_3m_ago': 'NUMERIC(14, 4)',
            'sma_200_4m_ago': 'NUMERIC(14, 4)',
            'sma_200_5m_ago': 'NUMERIC(14, 4)',
            'sma_30w': 'NUMERIC(14, 4)',
            'sma_40w': 'NUMERIC(14, 4)',
            'fifty_two_week_high': 'NUMERIC(14, 4)',
            'fifty_two_week_low':  'NUMERIC(14, 4)',
            'average_volume_50':   'NUMERIC(20, 2)',
            'price_minus_sma_10':  'NUMERIC(14, 4)',
            'price_minus_sma_21':  'NUMERIC(14, 4)',
            'price_minus_sma_50':  'NUMERIC(14, 4)',
            'price_minus_sma_150': 'NUMERIC(14, 4)',
            'price_minus_sma_200': 'NUMERIC(14, 4)',
            'price_vs_sma_10_percent':  'NUMERIC(14, 4)',
            'price_vs_sma_21_percent':  'NUMERIC(14, 4)',
            'price_vs_sma_50_percent':  'NUMERIC(14, 4)',
            'price_vs_sma_150_percent': 'NUMERIC(14, 4)',
            'price_vs_sma_200_percent': 'NUMERIC(14, 4)',
            'percent_off_52w_high': 'NUMERIC(14, 4)',
            'percent_off_52w_low':  'NUMERIC(14, 4)',
            'vol_diff_50_percent':  'NUMERIC(14, 4)',
            # Power Play columns
            'percent_change_15d':   'NUMERIC(14, 4)',
            'percent_change_20d':   'NUMERIC(14, 4)',
            'percent_change_126d':  'NUMERIC(14, 4)',
            'beta':                 'NUMERIC(12, 4)',
        }

        for col_name, col_type in columns_to_add.items():
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(
                        f"ALTER TABLE stock_indicators ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                    ))
                    logger.info(f"✅ تم التأكد من وجود العمود: {col_name}")
            except Exception as e:
                logger.warning(f"⚠️ تحذير: {col_name}: {e}")

    def load_data(self):
        """تحميل بيانات OHLCV من جدول prices"""
        query = """
        SELECT id, symbol, date, open, close, high, low, volume_traded
        FROM prices
        ORDER BY symbol, date
        """
        logger.info("⏳ جاري تحميل البيانات من قاعدة البيانات...")
        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        df['date'] = pd.to_datetime(df['date'])
        logger.info(f"✅ تم تحميل {len(df)} سجل.")
        return df

    def calculate(self, df):
        logger.info("📈 جاري حساب المؤشرات الفنية...")

        df = df.sort_values(['symbol', 'date'])

        # فلترة أيام العطلات
        columns_to_check = ['open', 'high', 'low', 'close']
        mask = (df[columns_to_check] != df.groupby('symbol')[columns_to_check].shift(1)).any(axis=1) | \
               (df.groupby('symbol')['date'].cumcount() == 0)
        df = df[mask].copy()

        grouped = df.groupby('symbol')

        # 1. SMAs اليومية
        for window in [10, 20, 21, 50, 100, 150, 200]:
            df[f'sma_{window}'] = grouped['close'].transform(
                lambda x: x.rolling(window=window).mean()
            )

        # 2. 52 Week High / Low
        df['fifty_two_week_high'] = grouped['high'].transform(
            lambda x: x.rolling(window=260).max()
        )
        df['fifty_two_week_low'] = grouped['low'].transform(
            lambda x: x.rolling(window=260).min()
        )

        # 3. Average Volume (50 days)
        df['average_volume_50'] = grouped['volume_traded'].transform(
            lambda x: x.rolling(window=50).mean()
        )

        # 4. Change
        logger.info("   ... حساب التغير (Change)")
        df['change'] = grouped['close'].transform(lambda x: x.diff())

        # 4b. Power Play: حساب التغيرات المطلوبة
        logger.info("   ... حساب percent_change_20d و percent_change_15d و percent_change_126d")
        df['percent_change_15d'] = grouped['close'].transform(
            lambda x: ((x - x.shift(15)) / x.shift(15).replace(0, np.nan)) * 100
        )
        df['percent_change_20d'] = grouped['close'].transform(
            lambda x: ((x - x.shift(20)) / x.shift(20).replace(0, np.nan)) * 100
        )
        df['percent_change_126d'] = grouped['close'].transform(
            lambda x: ((x - x.shift(126)) / x.shift(126).replace(0, np.nan)) * 100
        )

        # 5. Historical 200MA
        logger.info("   ... حساب 200MA التاريخية")
        for months_ago, days in [(1, 21), (2, 42), (3, 63), (4, 84), (5, 105)]:
            col_name = f'sma_200_{months_ago}m_ago'
            df[col_name] = grouped['close'].transform(
                lambda x, d=days: x.rolling(window=200).mean().shift(d)
            )

        # 6. 30W و 40W SMAs
        logger.info("   ... حساب 30W و 40W SMAs")
        df['week_ending'] = df['date'] + pd.to_timedelta((4 - df['date'].dt.dayofweek) % 7, unit='D')
        weekly_closes = df.groupby(['symbol', 'week_ending'])['close'].last().reset_index()
        weekly_closes = weekly_closes.sort_values(['symbol', 'week_ending'])
        weekly_closes['sma_30w_calc'] = weekly_closes.groupby('symbol')['close'].transform(
            lambda x: x.rolling(window=30).mean()
        )
        weekly_closes['sma_40w_calc'] = weekly_closes.groupby('symbol')['close'].transform(
            lambda x: x.rolling(window=40).mean()
        )
        df = df.merge(
            weekly_closes[['symbol', 'week_ending', 'sma_30w_calc', 'sma_40w_calc']],
            on=['symbol', 'week_ending'],
            how='left'
        )
        df['sma_30w'] = df['sma_30w_calc']
        df['sma_40w'] = df['sma_40w_calc']
        df = df.drop(['sma_30w_calc', 'sma_40w_calc', 'week_ending'], axis=1)

        # 7. النسب المئوية
        for window in [10, 20, 21, 50, 100, 150, 200]:
            col_sma = f'sma_{window}'
            df[f'price_vs_sma_{window}_percent'] = (
                (df['close'] - df[col_sma]) / df[col_sma].replace(0, np.nan)
            ) * 100

        df['percent_off_52w_high'] = (
            (df['close'] - df['fifty_two_week_high'].replace(0, np.nan)) /
            df['fifty_two_week_high'].replace(0, np.nan)
        ) * 100
        df['percent_off_52w_low'] = (
            (df['close'] - df['fifty_two_week_low'].replace(0, np.nan)) /
            df['fifty_two_week_low'].replace(0, np.nan)
        ) * 100
        df['vol_diff_50_percent'] = (
            (df['volume_traded'] - df['average_volume_50']) /
            df['average_volume_50'].replace(0, np.nan)
        ) * 100

        # 8. Beta Calculation vs TASI Benchmark
        logger.info("   ... حساب Beta (Volatility) vs Benchmark (TASI)")
        
        # 8a: Find the Benchmark
        # Saudi main index is often just "TASI" or "TASI.SR" or "^TASI"
        tasi_df = df[df['symbol'].isin(['TASI', 'TASI.SR', '^TASI', 'TASI.CM'])]
        if not tasi_df.empty:
            market_df = tasi_df.groupby('date')['close'].last().reset_index()
            market_df.rename(columns={'close': 'market_close'}, inplace=True)
            logger.info("   ✅ تم العثور على مؤشر TASI في البيانات ليستخدم כـ Benchmark")
        else:
            try:
                logger.info("   🌐 Fetching exact ^TASI.SR data from Yahoo Finance for 1 year...")
                url = "https://query1.finance.yahoo.com/v8/finance/chart/^TASI.SR?interval=1d&range=1y"
                headers = {'User-Agent': 'Mozilla/5.0'}
                res = requests.get(url, headers=headers, timeout=10)
                data = res.json()
                
                result = data.get('chart', {}).get('result')
                if not result:
                    raise Exception("Yahoo API returned no data for ^TASI.SR")

                timestamps = result[0].get('timestamp', [])
                quote = result[0].get('indicators', {}).get('quote', [{}])[0]
                closes = quote.get('close', [])
                
                dates = []
                clean_closes = []
                for ts, c in zip(timestamps, closes):
                    if c is not None:
                        dt = pd.to_datetime(ts, unit='s', utc=True).tz_convert('Asia/Riyadh')
                        dates.append(dt.date())
                        clean_closes.append(c)
                        
                meta = result[0].get('meta', {})
                latest_time = meta.get('regularMarketTime')
                latest_close = meta.get('regularMarketPrice')
                
                if latest_time is not None and latest_close is not None:
                    dt_latest = pd.to_datetime(latest_time, unit='s', utc=True).tz_convert('Asia/Riyadh').date()
                    if dt_latest not in dates:
                        logger.info(f"   ⚡ Fixing Yahoo Delay: Appending real-time today's data from Meta -> {dt_latest}")
                        dates.append(dt_latest)
                        clean_closes.append(latest_close)

                market_df = pd.DataFrame({'date': dates, 'market_close': clean_closes})
                market_df['date'] = pd.to_datetime(market_df['date'])
                logger.info(f"   ✅ تم تحميل بيانات TASI الفعلية من Yahoo بنجاح (آخر تاريخ: {market_df['date'].max().date()}).")
            except Exception as e:
                logger.error(f"   ❌ فشل سحب TASI من Yahoo: {e}")
                # Fallback: using equal-weighted market average
                market_df = df.groupby('date')['close'].mean().reset_index()
                market_df.rename(columns={'close': 'market_close'}, inplace=True)
                logger.info("   ⚠️ لم يتم العثور على مؤشر TASI صريح، تم حساب المؤشر العام تلقائياً")

        # Sort values properly for pct_change and rolling arithmetic
        df = df.sort_values(['symbol', 'date']).reset_index(drop=True)
        market_df = market_df.sort_values('date').reset_index(drop=True)

        df['stock_return'] = df.groupby('symbol')['close'].pct_change()
        market_df['market_return'] = market_df['market_close'].pct_change()
        # Compute market variance (same for all stocks on a given date)
        market_df['market_var'] = market_df['market_return'].rolling(window=260, min_periods=130).var()

        # Merge daily market data into df to align rows perfectly
        df = df.merge(market_df[['date', 'market_return', 'market_var']], on='date', how='left')

        # Compute stock covariance vs market
        def compute_covariance(sub_df):
            # sub_df maintains its original index from df, ensuring alignement
            return sub_df['stock_return'].rolling(window=260, min_periods=130).cov(sub_df['market_return'])

        df['cov_stock_market'] = df.groupby('symbol', group_keys=False).apply(compute_covariance)
        
        # Compute Beta
        df['beta'] = df['cov_stock_market'] / df['market_var']
        df['beta'] = df['beta'].replace([np.inf, -np.inf], np.nan)
        
        # Clean up temporary columns
        df.drop(['stock_return', 'market_return', 'market_var', 'cov_stock_market'], axis=1, inplace=True)

        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        return df

    def save_latest(self, df):
        """
        يحفظ:
          - حقل change فقط في جدول prices (الوحيد المتبقي)
          - جميع إحصائيات السوق (SMA, 52w, vol, %) في stock_indicators عبر UPSERT
        """
        logger.info("💾 جاري تحضير البيانات للحفظ...")

        latest_dates = df.groupby('symbol')['date'].max().reset_index()
        latest_data = pd.merge(df, latest_dates, on=['symbol', 'date'])

        logger.info(f"🚀 جاري تحديث {len(latest_data)} سهم...")

        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                for idx, row in latest_data.iterrows():
                    symbol   = row['symbol']
                    rec_date = row['date'].date() if hasattr(row['date'], 'date') else row['date']

                    # ─── 1. تحديث change في prices فقط ───────────────────────
                    conn.execute(
                        text("UPDATE prices SET change = :change WHERE id = :id"),
                        {
                            'change': round(float(row['change']), 2) if pd.notnull(row['change']) else None,
                            'id': int(row['id'])
                        }
                    )

                    # ─── 2. UPSERT في stock_indicators ────────────────────────
                    def fv(key):
                        """safe float value"""
                        val = row.get(key)
                        return float(val) if val is not None and pd.notnull(val) else None

                    def pm(a, b):
                        """price minus SMA"""
                        va, vb = row.get(a), row.get(b)
                        return float(va - vb) if va is not None and vb is not None \
                               and pd.notnull(va) and pd.notnull(vb) else None

                    si_params = {
                        'symbol':   symbol,
                        'date':     rec_date,
                        'sma_10':   fv('sma_10'),
                        'sma_21':   fv('sma_21'),
                        'sma_50':   fv('sma_50'),
                        'sma_150':  fv('sma_150'),
                        'sma_200':  fv('sma_200'),
                        'sma_200_1m': fv('sma_200_1m_ago'),
                        'sma_200_2m': fv('sma_200_2m_ago'),
                        'sma_200_3m': fv('sma_200_3m_ago'),
                        'sma_200_4m': fv('sma_200_4m_ago'),
                        'sma_200_5m': fv('sma_200_5m_ago'),
                        'sma_30w':  fv('sma_30w'),
                        'sma_40w':  fv('sma_40w'),
                        'h52':      fv('fifty_two_week_high'),
                        'l52':      fv('fifty_two_week_low'),
                        'avg_vol':  fv('average_volume_50'),
                        'pm10':  pm('close', 'sma_10'),
                        'pm21':  pm('close', 'sma_21'),
                        'pm50':  pm('close', 'sma_50'),
                        'pm150': pm('close', 'sma_150'),
                        'pm200': pm('close', 'sma_200'),
                        'p10_pct':   fv('price_vs_sma_10_percent'),
                        'p21_pct':   fv('price_vs_sma_21_percent'),
                        'p50_pct':   fv('price_vs_sma_50_percent'),
                        'p150_pct':  fv('price_vs_sma_150_percent'),
                        'p200_pct':  fv('price_vs_sma_200_percent'),
                        'pct_off_high': fv('percent_off_52w_high'),
                        'pct_off_low':  fv('percent_off_52w_low'),
                        'vol_diff':     fv('vol_diff_50_percent'),
                        'pct_chg_15d':  fv('percent_change_15d'),
                        'pct_chg_20d':  fv('percent_change_20d'),
                        'pct_chg_126d': fv('percent_change_126d'),
                        'beta':         fv('beta'),
                    }

                    conn.execute(text("""
                        INSERT INTO stock_indicators (
                            symbol, date,
                            sma_10, sma_21, sma_50, sma_150, sma_200,
                            sma_200_1m_ago, sma_200_2m_ago, sma_200_3m_ago, sma_200_4m_ago, sma_200_5m_ago,
                            sma_30w, sma_40w,
                            fifty_two_week_high, fifty_two_week_low, average_volume_50,
                            price_minus_sma_10, price_minus_sma_21, price_minus_sma_50,
                            price_minus_sma_150, price_minus_sma_200,
                            price_vs_sma_10_percent, price_vs_sma_21_percent, price_vs_sma_50_percent,
                            price_vs_sma_150_percent, price_vs_sma_200_percent,
                            percent_off_52w_high, percent_off_52w_low, vol_diff_50_percent,
                            percent_change_15d, percent_change_20d, percent_change_126d, beta
                        ) VALUES (
                            :symbol, :date,
                            :sma_10, :sma_21, :sma_50, :sma_150, :sma_200,
                            :sma_200_1m, :sma_200_2m, :sma_200_3m, :sma_200_4m, :sma_200_5m,
                            :sma_30w, :sma_40w,
                            :h52, :l52, :avg_vol,
                            :pm10, :pm21, :pm50, :pm150, :pm200,
                            :p10_pct, :p21_pct, :p50_pct, :p150_pct, :p200_pct,
                            :pct_off_high, :pct_off_low, :vol_diff,
                            :pct_chg_15d, :pct_chg_20d, :pct_chg_126d, :beta
                        )
                        ON CONFLICT (symbol, date) DO UPDATE SET
                            sma_10  = EXCLUDED.sma_10,
                            sma_21  = EXCLUDED.sma_21,
                            sma_50  = EXCLUDED.sma_50,
                            sma_150 = EXCLUDED.sma_150,
                            sma_200 = EXCLUDED.sma_200,
                            sma_200_1m_ago = EXCLUDED.sma_200_1m_ago,
                            sma_200_2m_ago = EXCLUDED.sma_200_2m_ago,
                            sma_200_3m_ago = EXCLUDED.sma_200_3m_ago,
                            sma_200_4m_ago = EXCLUDED.sma_200_4m_ago,
                            sma_200_5m_ago = EXCLUDED.sma_200_5m_ago,
                            sma_30w = EXCLUDED.sma_30w,
                            sma_40w = EXCLUDED.sma_40w,
                            fifty_two_week_high = EXCLUDED.fifty_two_week_high,
                            fifty_two_week_low  = EXCLUDED.fifty_two_week_low,
                            average_volume_50   = EXCLUDED.average_volume_50,
                            price_minus_sma_10  = EXCLUDED.price_minus_sma_10,
                            price_minus_sma_21  = EXCLUDED.price_minus_sma_21,
                            price_minus_sma_50  = EXCLUDED.price_minus_sma_50,
                            price_minus_sma_150 = EXCLUDED.price_minus_sma_150,
                            price_minus_sma_200 = EXCLUDED.price_minus_sma_200,
                            price_vs_sma_10_percent  = EXCLUDED.price_vs_sma_10_percent,
                            price_vs_sma_21_percent  = EXCLUDED.price_vs_sma_21_percent,
                            price_vs_sma_50_percent  = EXCLUDED.price_vs_sma_50_percent,
                            price_vs_sma_150_percent = EXCLUDED.price_vs_sma_150_percent,
                            price_vs_sma_200_percent = EXCLUDED.price_vs_sma_200_percent,
                            percent_off_52w_high = EXCLUDED.percent_off_52w_high,
                            percent_off_52w_low  = EXCLUDED.percent_off_52w_low,
                            vol_diff_50_percent  = EXCLUDED.vol_diff_50_percent,
                            percent_change_15d   = EXCLUDED.percent_change_15d,
                            percent_change_20d   = EXCLUDED.percent_change_20d,
                            percent_change_126d  = EXCLUDED.percent_change_126d,
                            beta                 = EXCLUDED.beta
                    """), si_params)

                trans.commit()
                logger.info("✅ تم حفظ الإحصائيات في stock_indicators بنجاح.")
            except Exception as e:
                trans.rollback()
                logger.error(f"❌ خطأ أثناء التحديث: {e}")
                raise

    def save_change_only_and_return_tech_map(self, df):
        """
        Atomic Pipeline Mode:
          1. Updates ONLY the 'change' column in prices table
          2. Returns a dict of {symbol: {sma_10: x, sma_50: y, ...}} for later merging
          3. Does NOT write to stock_indicators (that happens later in one atomic shot)
        """
        logger.info("💾 جاري تحضير البيانات (Atomic Mode - prices.change فقط)...")

        latest_dates = df.groupby('symbol')['date'].max().reset_index()
        latest_data = pd.merge(df, latest_dates, on=['symbol', 'date'])

        logger.info(f"🚀 جاري تحديث change لـ {len(latest_data)} سهم...")

        # 1. Update prices.change only
        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                for idx, row in latest_data.iterrows():
                    conn.execute(
                        text("UPDATE prices SET change = :change WHERE id = :id"),
                        {
                            'change': round(float(row['change']), 2) if pd.notnull(row['change']) else None,
                            'id': int(row['id'])
                        }
                    )
                trans.commit()
                logger.info("✅ تم تحديث change في prices بنجاح.")
            except Exception as e:
                trans.rollback()
                logger.error(f"❌ خطأ أثناء تحديث prices.change: {e}")
                raise

        # 2. Build tech_map: {symbol: {col: value, ...}}
        tech_map = {}
        for idx, row in latest_data.iterrows():
            symbol = row['symbol']
            rec_date = row['date'].date() if hasattr(row['date'], 'date') else row['date']

            def fv(key):
                val = row.get(key)
                return float(val) if val is not None and pd.notnull(val) else None

            def pm(a, b):
                va, vb = row.get(a), row.get(b)
                return float(va - vb) if va is not None and vb is not None \
                       and pd.notnull(va) and pd.notnull(vb) else None

            tech_map[symbol] = {
                'date': rec_date,
                'sma_10': fv('sma_10'),
                'sma_20': fv('sma_20'),
                'sma_21': fv('sma_21'),
                'sma_50': fv('sma_50'),
                'sma_100': fv('sma_100'),
                'sma_150': fv('sma_150'),
                'sma_200': fv('sma_200'),
                'sma_200_1m_ago': fv('sma_200_1m_ago'),
                'sma_200_2m_ago': fv('sma_200_2m_ago'),
                'sma_200_3m_ago': fv('sma_200_3m_ago'),
                'sma_200_4m_ago': fv('sma_200_4m_ago'),
                'sma_200_5m_ago': fv('sma_200_5m_ago'),
                'sma_30w': fv('sma_30w'),
                'sma_40w': fv('sma_40w'),
                'fifty_two_week_high': fv('fifty_two_week_high'),
                'fifty_two_week_low': fv('fifty_two_week_low'),
                'average_volume_50': fv('average_volume_50'),
                'price_minus_sma_10': pm('close', 'sma_10'),
                'price_minus_sma_20': pm('close', 'sma_20'),
                'price_minus_sma_21': pm('close', 'sma_21'),
                'price_minus_sma_50': pm('close', 'sma_50'),
                'price_minus_sma_100': pm('close', 'sma_100'),
                'price_minus_sma_150': pm('close', 'sma_150'),
                'price_minus_sma_200': pm('close', 'sma_200'),
                'price_vs_sma_10_percent': fv('price_vs_sma_10_percent'),
                'price_vs_sma_20_percent': fv('price_vs_sma_20_percent'),
                'price_vs_sma_21_percent': fv('price_vs_sma_21_percent'),
                'price_vs_sma_50_percent': fv('price_vs_sma_50_percent'),
                'price_vs_sma_100_percent': fv('price_vs_sma_100_percent'),
                'price_vs_sma_150_percent': fv('price_vs_sma_150_percent'),
                'price_vs_sma_200_percent': fv('price_vs_sma_200_percent'),
                'percent_off_52w_high': fv('percent_off_52w_high'),
                'percent_off_52w_low': fv('percent_off_52w_low'),
                'vol_diff_50_percent': fv('vol_diff_50_percent'),
                'percent_change_15d': fv('percent_change_15d'),
                'percent_change_20d': fv('percent_change_20d'),
                'percent_change_126d': fv('percent_change_126d'),
                'beta': fv('beta'),
            }

        logger.info(f"✅ تم تجهيز بيانات المؤشرات الفنية لـ {len(tech_map)} سهم (في الذاكرة).")
        return tech_map


if __name__ == "__main__":
    calc = TechnicalCalculator(str(settings.DATABASE_URL))
    df = calc.load_data()
    df_calc = calc.calculate(df)
    calc.save_latest(df_calc)