# import pandas as pd
# from datetime import timedelta, date
# from sqlalchemy.orm import Session
# import logging
# from app.models.price import Price
# from app.models.rs_daily import RSDaily

# logger = logging.getLogger(__name__)

# def get_price_at_date(df, target_date):
#     """
#     الحصول على السعر في تاريخ محدد أو أقرب تاريخ سابق
#     """
#     row = df[df['date'] <= target_date].iloc[-1:]
#     if row.empty:
#         return None
#     return row.iloc[0]['close']

# def calculate_rs_batch(db: Session, target_date: date):
#     """
#     حساب RS لكل الأسهم في تاريخ محدد
#     """
#     # 1. جلب بيانات السنة الماضية لكل الأسهم
#     # نحتاج بيانات سنة + هامش بسيط (370 يوم)
#     start_date = target_date - timedelta(days=370)
    
#     query = db.query(
#         Price.symbol,
#         Price.date,
#         Price.close
#     ).filter(
#         Price.date.between(start_date, target_date)
#     ).order_by(Price.symbol, Price.date)
    
#     # تحويل لـ DataFrame للمعالجة السريعة
#     # استخدام connection لجلب البيانات (متوافق مع SQLAlchemy 2.0)
#     with db.bind.connect() as connection:
#         df = pd.read_sql(query.statement, connection)
    
#     if df.empty:
#         logger.warning(f"⚠️ لا توجد بيانات لتاريخ {target_date}")
#         return 0
    
#     # تحويل التاريخ للتنسيق المناسب
#     df['date'] = pd.to_datetime(df['date']).dt.date
    
#     results = []
    
#     # 2. حساب العوائد لكل سهم
#     # Group by symbol
#     grouped = df.groupby('symbol')
    
#     for symbol, group in grouped:
#         try:
#             # السعر الحالي
#             current_row = group[group['date'] == target_date]
#             if current_row.empty:
#                 continue
            
#             current_price = float(current_row.iloc[0]['close'])
            
#             # حساب العوائد للفترات المختلفة
#             periods = {
#                 '3m': float(get_price_at_date(group, target_date - timedelta(days=90)) or 0),
#                 '6m': float(get_price_at_date(group, target_date - timedelta(days=180)) or 0),
#                 '9m': float(get_price_at_date(group, target_date - timedelta(days=270)) or 0),
#                 '12m': float(get_price_at_date(group, target_date - timedelta(days=365)) or 0)
#             }
            
#             # تجاهل السهم لو مفيش بيانات سنة كاملة (سعر 12 شهر = 0)
#             if periods['12m'] == 0:
#                 continue
                
#             returns = {
#                 '3m': (current_price / periods['3m']) - 1 if periods['3m'] > 0 else 0,
#                 '6m': (current_price / periods['6m']) - 1 if periods['6m'] > 0 else 0,
#                 '9m': (current_price / periods['9m']) - 1 if periods['9m'] > 0 else 0,
#                 '12m': (current_price / periods['12m']) - 1 if periods['12m'] > 0 else 0
#             }
            
#             # حساب الأداء الموزون (Weighted Performance)
#             weighted_perf = (
#                 (returns['3m'] * 0.4) +
#                 (returns['6m'] * 0.2) +
#                 (returns['9m'] * 0.2) +
#                 (returns['12m'] * 0.2)
#             ) * 100
            
#             results.append({
#                 'symbol': symbol,
#                 'return_3m': returns['3m'],
#                 'return_6m': returns['6m'],
#                 'return_9m': returns['9m'],
#                 'return_12m': returns['12m'],
#                 'weighted_performance': weighted_perf
#             })
            
#         except Exception as e:
#             logger.error(f"Error calculating for {symbol}: {e}")
#             continue

#     if not results:
#         return 0

#     # 3. حساب RS بالطريقة القديمة (webScraping Style)
#     # حساب Percentile لكل فترة لوحدها، ثم المتوسط الموزون
#     results_df = pd.DataFrame(results)
    
#     # حساب RS Percentile لكل فترة على حدة
#     # Formula: MIN(ROUND(PERCENTRANK.INC * 100, 0), 99)
#     results_df['rs_3m'] = (results_df['return_3m'].rank(pct=True) * 100).round(0).clip(upper=99)
#     results_df['rs_6m'] = (results_df['return_6m'].rank(pct=True) * 100).round(0).clip(upper=99)
#     results_df['rs_9m'] = (results_df['return_9m'].rank(pct=True) * 100).round(0).clip(upper=99)
#     results_df['rs_12m'] = (results_df['return_12m'].rank(pct=True) * 100).round(0).clip(upper=99)
    
#     # حساب RS النهائي = متوسط موزون للـ Ranks
#     # Weights: 3M (40%), 6M (20%), 9M (20%), 12M (20%)
#     import numpy as np
#     results_df['rs_percentile'] = np.ceil(
#         (results_df['rs_3m'] * 0.4) +
#         (results_df['rs_6m'] * 0.2) +
#         (results_df['rs_9m'] * 0.2) +
#         (results_df['rs_12m'] * 0.2)
#     ).clip(1, 99)
    
#     # حساب rs_raw (نفس القيمة للتوافق مع الجدول)
#     results_df['rs_raw'] = results_df['rs_percentile']
    
#     # إضافة الترتيب
#     results_df = results_df.sort_values('rs_percentile', ascending=False)
#     results_df['rank_position'] = range(1, len(results_df) + 1)
    
#     # 4. حفظ النتائج في قاعدة البيانات
#     processed_count = 0
#     for _, row in results_df.iterrows():
#         # البحث عن سجل موجود
#         existing_record = db.query(RSDaily).filter(
#             RSDaily.symbol == row['symbol'],
#             RSDaily.date == target_date
#         ).first()

#         if existing_record:
#             # تحديث
#             existing_record.return_3m = row['return_3m']
#             existing_record.return_6m = row['return_6m']
#             existing_record.return_9m = row['return_9m']
#             existing_record.return_12m = row['return_12m']
#             existing_record.rs_raw = row['rs_raw']
#             existing_record.rs_percentile = row['rs_percentile']
#             existing_record.rank_position = row['rank_position']
#             existing_record.total_stocks = len(results_df)
#         else:
#             # إنشاء جديد
#             rs_record = RSDaily(
#                 symbol=row['symbol'],
#                 date=target_date,
#                 return_3m=row['return_3m'],
#                 return_6m=row['return_6m'],
#                 return_9m=row['return_9m'],
#                 return_12m=row['return_12m'],
#                 rs_raw=row['rs_raw'],
#                 rs_percentile=row['rs_percentile'],
#                 rank_position=row['rank_position'],
#                 total_stocks=len(results_df)
#             )
#             db.add(rs_record)
            
#         processed_count += 1
    
#     db.commit()
#     logger.info(f"✅ تم حساب RS لـ {processed_count} سهم في {target_date}")
#     return processed_count
