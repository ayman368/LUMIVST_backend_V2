import sys
from pathlib import Path
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import logging

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
        """تأكد من وجود الأعمدة الجديدة، وإن لم تكن موجودة أضفها"""
        logger.info("🔍 التحقق من وجود الأعمدة الجديدة...")
        
        columns_to_add = {
            'ema_21': 'NUMERIC(12, 2)',
            'ema_10': 'NUMERIC(12, 2)',
            'sma_3': 'NUMERIC(12, 2)',
            'ema_20_sma3': 'NUMERIC(12, 2)',
            'sma_4': 'NUMERIC(12, 2)',
            'sma_9': 'NUMERIC(12, 2)',
            'sma_18': 'NUMERIC(12, 2)',
            'sma_4w': 'NUMERIC(12, 2)',
            'sma_9w': 'NUMERIC(12, 2)',
            'sma_18w': 'NUMERIC(12, 2)',
            'sma_200_1m_ago': 'NUMERIC(12, 2)',
            'sma_200_2m_ago': 'NUMERIC(12, 2)',
            'sma_200_3m_ago': 'NUMERIC(12, 2)',
            'sma_200_4m_ago': 'NUMERIC(12, 2)',
            'sma_200_5m_ago': 'NUMERIC(12, 2)',
            'sma_30w': 'NUMERIC(12, 2)',
            'sma_40w': 'NUMERIC(12, 2)',
            'cci_14': 'NUMERIC(12, 2)',
            'cci_ema_20': 'NUMERIC(12, 2)',
            'aroon_up': 'NUMERIC(12, 2)',
            'aroon_down': 'NUMERIC(12, 2)',
        }
        
        # إضافة كل عمود في اتصال منفصل لتجنب مشاكل التTransactions
        for col_name, col_type in columns_to_add.items():
            try:
                with self.engine.begin() as conn:
                    alter_query = f"ALTER TABLE prices ADD COLUMN {col_name} {col_type}"
                    conn.execute(text(alter_query))
                    logger.info(f"✅ تم إضافة العمود: {col_name}")
            except Exception as e:
                if "already exists" in str(e) or "duplicate" in str(e).lower():
                    logger.info(f"✓ العمود {col_name} موجود بالفعل")
                else:
                    logger.warning(f"⚠️ تحذير: {col_name}: {e}")

    def load_data(self):
        # سحب كل الأعمدة اللازمة للحسابات بما فيها الـ high والـ low و open
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
        logger.info("📈 جاري حساب المؤشرات الفنية (القيم الكاملة)...")
        
        # ترتيب البيانات لضمان دقة العمليات الحسابية المتسلسلة
        df = df.sort_values(['symbol', 'date'])
        
        # فلترة أيام العطلات (التي ينسخ فيها مزود البيانات اليوم السابق بالظبط)
        # لتتطابق المؤشرات مثل SMA و 52 Week High مع TradingView
        columns_to_check = ['open', 'high', 'low', 'close']
        mask = (df[columns_to_check] != df.groupby('symbol')[columns_to_check].shift(1)).any(axis=1) | (df.groupby('symbol')['date'].cumcount() == 0)
        df = df[mask].copy()
        
        grouped = df.groupby('symbol')

        # 1. حساب قيم المتوسطات المتحركة (SMA) مباشرة
        # الحساب يعتمد على سعر الإغلاق (Close) لآخر X يوم
        for window in [10, 21, 50, 150, 200]:
            df[f'sma_{window}'] = grouped['close'].transform(lambda x: x.rolling(window=window).mean())

        # 2. حساب الـ 52 Week High من عمود الـ High (أعلى سعر وصل له السهم - 260 يوم)
        df['fifty_two_week_high'] = grouped['high'].transform(lambda x: x.rolling(window=260).max())

        # 3. حساب الـ 52 Week Low من عمود الـ Low (أقل سعر وصل له السهم - 260 يوم)
        df['fifty_two_week_low'] = grouped['low'].transform(lambda x: x.rolling(window=260).min())

        # 4. حساب متوسط حجم التداول لـ 50 يوم (Average Volume)
        df['average_volume_50'] = grouped['volume_traded'].transform(lambda x: x.rolling(window=50).mean())

        # 5. حساب التغير (Change) = سعر إغلاق اليوم - سعر إغلاق أمس
        logger.info("   ... حساب التغير (Change)")
        df['change'] = grouped['close'].transform(lambda x: x.diff())

        # 6. حساب 21-Day EMA (TradingView Exact Match)
        logger.info("   ... حساب 21-Day و 10-Day EMA (PineScript)")
        
        def calc_ema_tv_real(series, period):
            import numpy as np
            vals = series.values
            ema_vals = [np.nan] * len(vals)
            alpha = 2.0 / (period + 1.0)
            
            # Find the first non-NaN value to start the period window
            first_valid_idx = series.first_valid_index()
            if first_valid_idx is None:
                return pd.Series(ema_vals, index=series.index)
                
            start_idx = series.index.get_loc(first_valid_idx)
            
            # Need at least 'period' elements to calculate the first EMA (which is an SMA)
            if len(vals) - start_idx < period:
                return pd.Series(ema_vals, index=series.index)
                
            # First EMA is SMA of the first 'period' valid elements
            first_ema_idx = start_idx + period - 1
            ema_vals[first_ema_idx] = np.mean(vals[start_idx : first_ema_idx + 1])
            
            # Calculate the rest using the EMA formula
            for i in range(first_ema_idx + 1, len(vals)):
                if np.isnan(vals[i]):
                    ema_vals[i] = ema_vals[i-1] # Carry forward or skip based on preference. TVs carries forward.
                else:
                    ema_vals[i] = (vals[i] - ema_vals[i-1]) * alpha + ema_vals[i-1]
                    
            return pd.Series(ema_vals, index=series.index)

        df['ema_21'] = grouped['close'].transform(lambda x: calc_ema_tv_real(x, 21))

        # 6.1 حساب 10-Day EMA
        df['ema_10'] = grouped['close'].transform(lambda x: calc_ema_tv_real(x, 10))

        # 6.2 حساب 20-Day EMA و 3-Day SMA (EMA20(SMA3))
        logger.info("   ... حساب EMA20(SMA3)")
        df['sma_3'] = grouped['close'].transform(lambda x: x.rolling(window=3).mean())
        df['ema_20_sma3'] = grouped['sma_3'].transform(lambda x: calc_ema_tv_real(x, 20))

        # 7. حساب قيم 200MA التاريخية (للمقارنات)
        logger.info("   ... حساب 200MA التاريخية")
        # 1 month ≈ 21 trading days, 2 months ≈ 42, etc.
        for months_ago, days in [(1, 21), (2, 42), (3, 63), (4, 84), (5, 105)]:
            # حساب 200MA أولاً، ثم إزاحتها للخلف (shift) للحصول على قيمة X يوم ماضي
            col_name = f'sma_200_{months_ago}m_ago'
            df[col_name] = grouped['close'].transform(
                lambda x, d=days: x.rolling(window=200).mean().shift(d)
            )

        # 8. حساب المتوسطات المتحركة الأسبوعية (Weekly SMAs)
        logger.info("   ... حساب Weekly SMAs")
        
        # تحويل التاريخ إلى week-ending dates للحصول على إغلاقات أسبوعية
        df['week_ending'] = df['date'] + pd.to_timedelta((4 - df['date'].dt.dayofweek) % 7, unit='D')
        
        # للحصول على آخر إغلاق في كل أسبوع
        weekly_closes = df.groupby(['symbol', 'week_ending'])['close'].last().reset_index()
        weekly_closes = weekly_closes.sort_values(['symbol', 'week_ending'])
        
        # حساب 30W SMA و 40W SMA
        weekly_closes['sma_30w_calc'] = weekly_closes.groupby('symbol')['close'].transform(
            lambda x: x.rolling(window=30).mean()
        )
        weekly_closes['sma_40w_calc'] = weekly_closes.groupby('symbol')['close'].transform(
            lambda x: x.rolling(window=40).mean()
        )
        
        # دمج البيانات الأسبوعية مع البيانات اليومية
        # نستخدم آخر قيمة أسبوعية معروفة لكل تاريخ
        df = df.merge(
            weekly_closes[['symbol', 'week_ending', 'sma_30w_calc', 'sma_40w_calc']],
            left_on=['symbol', 'week_ending'],
            right_on=['symbol', 'week_ending'],
            how='left'
        )
        df['sma_30w'] = df['sma_30w_calc']
        df['sma_40w'] = df['sma_40w_calc']
        df = df.drop(['sma_30w_calc', 'sma_40w_calc', 'week_ending'], axis=1)

        # 9. حساب SMA 4, 9, 18 (يومي وأسبوعي)
        logger.info("   ... حساب SMA 4, 9, 18")
        for window in [4, 9, 18]:
            df[f'sma_{window}'] = grouped['close'].transform(lambda x, w=window: x.rolling(window=w).mean())

        # حساب الإصدارات الأسبوعية من SMA 4, 9, 18
        df['week_ending'] = df['date'] + pd.to_timedelta((4 - df['date'].dt.dayofweek) % 7, unit='D')
        weekly_data = df.groupby(['symbol', 'week_ending']).agg({
            'close': 'last',
            'high': 'max',
            'low': 'min'
        }).reset_index()
        
        for window in [4, 9, 18]:
            weekly_data[f'sma_{window}w_calc'] = weekly_data.groupby('symbol')['close'].transform(
                lambda x, w=window: x.rolling(window=w).mean()
            )
        
        # دمج البيانات الأسبوعية مع البيانات اليومية
        df = df.merge(
            weekly_data[['symbol', 'week_ending', 'sma_4w_calc', 'sma_9w_calc', 'sma_18w_calc']],
            left_on=['symbol', 'week_ending'],
            right_on=['symbol', 'week_ending'],
            how='left'
        )
        
        for window in [4, 9, 18]:
            df[f'sma_{window}w'] = df[f'sma_{window}w_calc']
            df = df.drop(f'sma_{window}w_calc', axis=1)
        
        df = df.drop('week_ending', axis=1)

        # 10. حساب CCI(14) - Commodity Channel Index
        logger.info("   ... حساب CCI(14)")
        # Typical Price = (High + Low + Close) / 3
        df['cci_14'] = 0.0  # Initialize
        df['cci_ema_20'] = 0.0  # Initialize
        df['aroon_up'] = 0.0  # Initialize
        df['aroon_down'] = 0.0  # Initialize
        
        for symbol in df['symbol'].unique():
            symbol_df = df[df['symbol'] == symbol].copy()
            
            # CCI Calculation
            tp = (symbol_df['high'] + symbol_df['low'] + symbol_df['close']) / 3
            tp_sma = tp.rolling(14).mean()
            tp_dev = tp.rolling(14).apply(lambda x: (x - x.mean()).abs().mean(), raw=False)
            cci_vals = (tp - tp_sma) / (0.015 * tp_dev.replace(0, np.nan))
            df.loc[symbol_df.index, 'cci_14'] = cci_vals
            
            # CCI EMA(20)
            cci_ema = cci_vals.ewm(span=20, adjust=False).mean()
            df.loc[symbol_df.index, 'cci_ema_20'] = cci_ema
            
            # Aroon (25-period) - Pine Script exact match
            # aroonUp = 100 * (period - barssince(high == highest(high, period))) / period
            # barssince=0 means current bar → aroonUp=100
            high_vals = symbol_df['high'].values
            aroon_up_vals = []
            for i in range(len(high_vals)):
                if i < 25:
                    aroon_up_vals.append(np.nan)
                else:
                    # window: i-25 to i inclusive (26 bars = period+1)
                    window = high_vals[i-25:i+1]
                    days_since_high = np.argmax(window[::-1])  # 0 = current bar
                    aroon_up_vals.append((25 - days_since_high) / 25 * 100)
            df.loc[symbol_df.index, 'aroon_up'] = aroon_up_vals

            low_vals = symbol_df['low'].values
            aroon_down_vals = []
            for i in range(len(low_vals)):
                if i < 25:
                    aroon_down_vals.append(np.nan)
                else:
                    window = low_vals[i-25:i+1]
                    days_since_low = np.argmin(window[::-1])  # 0 = current bar
                    aroon_down_vals.append((25 - days_since_low) / 25 * 100)
            df.loc[symbol_df.index, 'aroon_down'] = aroon_down_vals

        # 5. حساب النسب المئوية (للفلترة والعرض المتقدم)
        # نسبة ابتعاد السعر عن المتوسطات
        for window in [10, 21, 50, 150, 200]:
            col_sma = f'sma_{window}'
            df[f'price_vs_sma_{window}_percent'] = ((df['close'] - df[col_sma]) / df[col_sma].replace(0, np.nan)) * 100
        
        # نسبة ابتعاد السعر عن EMAs
        for window in [10, 21]:
            col_ema = f'ema_{window}'
            df[f'price_vs_ema_{window}_percent'] = ((df['close'] - df[col_ema]) / df[col_ema].replace(0, np.nan)) * 100
        
        # نسبة الابتعاد عن القمة والقاع السنوي
        df['percent_off_52w_high'] = ((df['close'] - df['fifty_two_week_high'].replace(0, np.nan)) / df['fifty_two_week_high'].replace(0, np.nan)) * 100
        df['percent_off_52w_low'] = ((df['close'] - df['fifty_two_week_low'].replace(0, np.nan)) / df['fifty_two_week_low'].replace(0, np.nan)) * 100
        
        # نسبة تغير حجم التداول عن المتوسط
        df['vol_diff_50_percent'] = ((df['volume_traded'] - df['average_volume_50']) / df['average_volume_50'].replace(0, np.nan)) * 100

        # تنظيف البيانات (بدون تقريب - احتفظ بالدقة الكاملة)
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        return df

    def save_latest(self, df):
        """تحديث السجلات الأخيرة فقط في قاعدة البيانات لضمان السرعة"""
        logger.info("💾 جاري تحضير البيانات للحفظ...")
        
        latest_dates = df.groupby('symbol')['date'].max().reset_index()
        latest_data = pd.merge(df, latest_dates, on=['symbol', 'date'])
        
        logger.info(f"🚀 جاري تحديث {len(latest_data)} سهم...")
        
        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                for idx, row in latest_data.iterrows():
                    update_stmt = text("""
                        UPDATE prices
                        SET change = :change,
                            sma_10 = :sma_10,
                            sma_21 = :sma_21,
                            sma_50 = :sma_50,
                            sma_150 = :sma_150,
                            sma_200 = :sma_200,
                            price_minus_sma_10 = :p_minus_10,
                            price_minus_sma_21 = :p_minus_21,
                            price_minus_sma_50 = :p_minus_50,
                            price_minus_sma_150 = :p_minus_150,
                            price_minus_sma_200 = :p_minus_200,
                            fifty_two_week_high = :h52,
                            fifty_two_week_low = :l52,
                            average_volume_50 = :avg_vol,
                            price_vs_sma_10_percent = :p10_pct,
                            price_vs_sma_21_percent = :p21_pct,
                            price_vs_sma_50_percent = :p50_pct,
                            price_vs_sma_150_percent = :p150_pct,
                            price_vs_sma_200_percent = :p200_pct,
                            price_vs_ema_10_percent = :p10_ema_pct,
                            price_vs_ema_21_percent = :p21_ema_pct,
                            percent_off_52w_high = :pct_off_high,
                            percent_off_52w_low = :pct_off_low,
                            vol_diff_50_percent = :vol_diff_pct,
                            ema_21 = :ema_21,
                            ema_10 = :ema_10,
                            sma_3 = :sma_3,
                            ema_20_sma3 = :ema_20_sma3,
                            sma_4 = :sma_4,
                            sma_9 = :sma_9,
                            sma_18 = :sma_18,
                            sma_4w = :sma_4w,
                            sma_9w = :sma_9w,
                            sma_18w = :sma_18w,
                            sma_200_1m_ago = :sma_200_1m,
                            sma_200_2m_ago = :sma_200_2m,
                            sma_200_3m_ago = :sma_200_3m,
                            sma_200_4m_ago = :sma_200_4m,
                            sma_200_5m_ago = :sma_200_5m,
                            sma_30w = :sma_30w,
                            sma_40w = :sma_40w,
                            cci_14 = :cci_14,
                            cci_ema_20 = :cci_ema_20,
                            aroon_up = :aroon_up,
                            aroon_down = :aroon_down
                        WHERE id = :id
                    """)
                    
                    params = {
                        'change': round(row['change'], 2) if pd.notnull(row['change']) else None,
                        'sma_10': row['sma_10'] if pd.notnull(row['sma_10']) else None,
                        'sma_21': row['sma_21'] if pd.notnull(row['sma_21']) else None,
                        'sma_50': row['sma_50'] if pd.notnull(row['sma_50']) else None,
                        'sma_150': row['sma_150'] if pd.notnull(row['sma_150']) else None,
                        'sma_200': row['sma_200'] if pd.notnull(row['sma_200']) else None,
                        'p_minus_10': float(row['close'] - row['sma_10']) if pd.notnull(row['close']) and pd.notnull(row['sma_10']) else None,
                        'p_minus_21': float(row['close'] - row['sma_21']) if pd.notnull(row['close']) and pd.notnull(row['sma_21']) else None,
                        'p_minus_50': float(row['close'] - row['sma_50']) if pd.notnull(row['close']) and pd.notnull(row['sma_50']) else None,
                        'p_minus_150': float(row['close'] - row['sma_150']) if pd.notnull(row['close']) and pd.notnull(row['sma_150']) else None,
                        'p_minus_200': float(row['close'] - row['sma_200']) if pd.notnull(row['close']) and pd.notnull(row['sma_200']) else None,
                        'h52': row['fifty_two_week_high'] if pd.notnull(row['fifty_two_week_high']) else None,
                        'l52': row['fifty_two_week_low'] if pd.notnull(row['fifty_two_week_low']) else None,
                        'avg_vol': int(row['average_volume_50']) if pd.notnull(row['average_volume_50']) else 0,
                        'p10_pct': row['price_vs_sma_10_percent'] if pd.notnull(row['price_vs_sma_10_percent']) else None,
                        'p21_pct': row['price_vs_sma_21_percent'] if pd.notnull(row['price_vs_sma_21_percent']) else None,
                        'p50_pct': row['price_vs_sma_50_percent'] if pd.notnull(row['price_vs_sma_50_percent']) else None,
                        'p150_pct': row['price_vs_sma_150_percent'] if pd.notnull(row['price_vs_sma_150_percent']) else None,
                        'p200_pct': row['price_vs_sma_200_percent'] if pd.notnull(row['price_vs_sma_200_percent']) else None,
                        'p10_ema_pct': row['price_vs_ema_10_percent'] if pd.notnull(row['price_vs_ema_10_percent']) else None,
                        'p21_ema_pct': row['price_vs_ema_21_percent'] if pd.notnull(row['price_vs_ema_21_percent']) else None,
                        'pct_off_high': row['percent_off_52w_high'] if pd.notnull(row['percent_off_52w_high']) else None,
                        'pct_off_low': row['percent_off_52w_low'] if pd.notnull(row['percent_off_52w_low']) else None,
                        'vol_diff_pct': row['vol_diff_50_percent'] if pd.notnull(row['vol_diff_50_percent']) else None,
                        'ema_21': row['ema_21'] if pd.notnull(row['ema_21']) else None,
                        'ema_10': row['ema_10'] if pd.notnull(row['ema_10']) else None,
                        'sma_3': row['sma_3'] if pd.notnull(row['sma_3']) else None,
                        'ema_20_sma3': row['ema_20_sma3'] if pd.notnull(row['ema_20_sma3']) else None,
                        'sma_4': row['sma_4'] if pd.notnull(row['sma_4']) else None,
                        'sma_9': row['sma_9'] if pd.notnull(row['sma_9']) else None,
                        'sma_18': row['sma_18'] if pd.notnull(row['sma_18']) else None,
                        'sma_4w': row['sma_4w'] if pd.notnull(row['sma_4w']) else None,
                        'sma_9w': row['sma_9w'] if pd.notnull(row['sma_9w']) else None,
                        'sma_18w': row['sma_18w'] if pd.notnull(row['sma_18w']) else None,
                        'sma_200_1m': row['sma_200_1m_ago'] if pd.notnull(row['sma_200_1m_ago']) else None,
                        'sma_200_2m': row['sma_200_2m_ago'] if pd.notnull(row['sma_200_2m_ago']) else None,
                        'sma_200_3m': row['sma_200_3m_ago'] if pd.notnull(row['sma_200_3m_ago']) else None,
                        'sma_200_4m': row['sma_200_4m_ago'] if pd.notnull(row['sma_200_4m_ago']) else None,
                        'sma_200_5m': row['sma_200_5m_ago'] if pd.notnull(row['sma_200_5m_ago']) else None,
                        'sma_30w': row['sma_30w'] if pd.notnull(row['sma_30w']) else None,
                        'sma_40w': row['sma_40w'] if pd.notnull(row['sma_40w']) else None,
                        'cci_14': row['cci_14'] if pd.notnull(row['cci_14']) else None,
                        'cci_ema_20': row['cci_ema_20'] if pd.notnull(row['cci_ema_20']) else None,
                        'aroon_up': row['aroon_up'] if pd.notnull(row['aroon_up']) else None,
                        'aroon_down': row['aroon_down'] if pd.notnull(row['aroon_down']) else None,
                        'id': row['id']
                    }
                    conn.execute(update_stmt, params)
                trans.commit()
                logger.info("✅ تم تحديث جميع المؤشرات بنجاح.")
            except Exception as e:
                trans.rollback()
                logger.error(f"❌ خطأ أثناء التحديث: {e}")
                raise

if __name__ == "__main__":
    calc = TechnicalCalculator(str(settings.DATABASE_URL))
    df = calc.load_data()
    df_calc = calc.calculate(df)
    calc.save_latest(df_calc)