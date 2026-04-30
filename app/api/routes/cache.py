from fastapi import APIRouter, HTTPException, Query
from app.core.redis import redis_cache

import asyncio

router = APIRouter(prefix="/cache", tags=["Cache Management"])

@router.post("/clear/all")
async def clear_all_cache():
    """مسح كل الكاش"""
    try:
        await redis_cache.flush_all()
        return {"message": "✅ تم مسح كل الكاش بنجاح"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ في مسح الكاش: {str(e)}")

@router.post("/clear/stocks")
async def clear_stocks_cache():
    """مسح كاش الأسهم"""
    try:
        # مسح كل مفاتيح الأسهم من Redis
        keys = await redis_cache.keys("tadawul_stocks:*")
        deleted = 0
        for key in keys:
            deleted += await redis_cache.delete(key)
        return {"message": f"✅ تم مسح كاش الأسهم بنجاح ({deleted} مفتاح)"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ في مسح كاش الأسهم: {str(e)}")

@router.post("/clear/financials")
async def clear_financial_cache(
    symbol: str = Query(None, description="رمز سهم واحد أو رموز متعددة مفصولة بفواصل")
):
    """مسح كاش البيانات المالية لرمز أو رموز محددة"""
    try:
        if symbol:
            # مسح كاش رموز محددة
            symbols = [s.strip() for s in symbol.split(',')]
            deleted = 0
            for sym in symbols:
                pattern = f"financials:*:{sym}:*"
                keys = await redis_cache.keys(pattern)
                for key in keys:
                    deleted += await redis_cache.delete(key)
            
            if len(symbols) > 1:
                message = f"✅ تم مسح كاش البيانات المالية لـ {len(symbols)} رمز"
            else:
                message = f"✅ تم مسح كاش البيانات المالية لـ {symbol}"
        else:
            # مسح كل كاش البيانات المالية
            keys = await redis_cache.keys("financials:*")
            deleted = 0
            for key in keys:
                deleted += await redis_cache.delete(key)
            message = "✅ تم مسح كاش البيانات المالية بالكامل"
            
        return {"message": message, "deleted_count": deleted}
    except Exception as e:
        print(f"❌ خطأ في مسح كاش البيانات المالية: {e}")
        raise HTTPException(status_code=500, detail=f"خطأ في مسح كاش البيانات المالية: {str(e)}")

@router.get("/status")
async def cache_status():
    """الحصول على حالة الكاش"""
    try:
        # اختبار اتصال Redis
        is_connected = redis_cache.redis_client is not None
        if is_connected:
            try:
                await redis_cache.redis_client.ping()
                status = "connected"
            except:
                status = "disconnected"
        else:
            status = "disconnected"
        
        return {
            "redis_status": status,
            "message": "✅ نظام الكاش يعمل بشكل طبيعي" if status == "connected" else "❌ نظام الكاش غير متاح"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ في التحقق من حالة الكاش: {str(e)}")

@router.delete("/clear/symbols")
async def clear_specific_symbols_cache(
    symbols: str = Query(..., description="رموز الأسهم مفصولة بفواصل"),
):
    """مسح كاش رموز محددة من Redis"""
    try:
        symbol_list = [s.strip() for s in symbols.split(",")]
        
        cleared_count = 0
        for symbol in symbol_list:
            clean_sym = ''.join(filter(str.isdigit, symbol)).upper()
            
            # مسح من Redis للأسهم
            cache_key = f"tadawul_stocks:symbol:{clean_sym}:country:Saudi Arabia"
            await redis_cache.delete(cache_key)
            
            # مسح كاش البيانات المالية
            fin_keys = await redis_cache.keys(f"financials:*:{clean_sym}:*")
            for key in fin_keys:
                await redis_cache.delete(key)
            
            cleared_count += 1
            print(f"🧹 تم مسح كاش {clean_sym}")
        
        return {
            "message": f"✅ تم مسح كاش {cleared_count} رمز",
            "cleared_symbols": symbol_list,
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ في مسح الكاش: {str(e)}")

@router.get("/stats")
async def cache_stats():
    """إحصائيات الكاش"""
    try:
        # جلب كل مفاتيح الـ stocks
        stock_keys = await redis_cache.keys("tadawul_stocks:*")
        financial_keys = await redis_cache.keys("financials:*")
        
        # تصنيف المفاتيح
        symbol_keys = [k for k in stock_keys if "symbol:" in k]
        bulk_keys = [k for k in stock_keys if "bulk:" in k]
        page_keys = [k for k in stock_keys if "page:" in k]
        all_keys = [k for k in stock_keys if "all:" in k]
        
        # مفاتيح البيانات المالية
        income_keys = [k for k in financial_keys if "income:" in k]
        balance_keys = [k for k in financial_keys if "balance:" in k]
        cashflow_keys = [k for k in financial_keys if "cashflow:" in k]
        
        return {
            "total_stock_keys": len(stock_keys),
            "symbol_keys": len(symbol_keys),
            "bulk_keys": len(bulk_keys),
            "page_keys": len(page_keys),
            "all_keys": len(all_keys),
            "total_financial_keys": len(financial_keys),
            "income_keys": len(income_keys),
            "balance_keys": len(balance_keys),
            "cashflow_keys": len(cashflow_keys),
            "sample_stock_keys": stock_keys[:3],
            "sample_financial_keys": financial_keys[:3]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ في جلب إحصائيات الكاش: {str(e)}")