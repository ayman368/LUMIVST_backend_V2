from fastapi import APIRouter, HTTPException, Query
from app.core.redis import redis_cache


router = APIRouter(prefix="/stocks", tags=["Tadawul Stocks"])


# ============ Cache Management ============

@router.delete("/clear-redis")
async def clear_redis_cache(
    pattern: str = Query("tadawul:*", description="نمط المفاتيح للمسح. افتراضي: tadawul:*")
):
    """
    🗑️ مسح جميع مفاتيح Redis المتعلقة بالأسهم
    """
    try:
        keys = await redis_cache.keys(pattern)
        
        if not keys:
            return {"success": True, "message": "لم يتم العثور على مفاتيح مطابقة", "deleted_count": 0}
        
        deleted_count = 0
        for key in keys:
            deleted = await redis_cache.delete(key)
            deleted_count += deleted
        
        print(f"🗑️ Redis: deleted {deleted_count} keys matching '{pattern}'")
        
        return {
            "success": True,
            "message": f"تم مسح {deleted_count} مفتاح Redis",
            "pattern": pattern,
            "deleted_keys": keys
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ في مسح Redis: {str(e)}")
