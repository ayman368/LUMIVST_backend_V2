from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime

from app.core.database import get_db
from app.models.user import User
from app.schemas.auth import UserResponse
from app.api.deps import get_current_admin
from app.core.redis import redis_cache
from app.core.limiter import limiter

router = APIRouter(prefix="/admin", tags=["Admin"])

@router.get("/users", response_model=List[UserResponse])
async def list_users(
    skip: int = 0, 
    limit: int = 100, 
    approved: bool = None,
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """عرض قائمة المستخدمين (للمدير فقط)"""
    query = db.query(User)
    
    if approved is not None:
        query = query.filter(User.is_approved == approved)
        
    users = query.offset(skip).limit(limit).all()
    return users

@router.get("/pending-users", response_model=List[UserResponse])
async def get_pending_users(
    skip: int = 0,
    limit: int = 100,
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """عرض المستخدمين المنتظرين للموافقة"""
    users = db.query(User).filter(User.is_approved == False).offset(skip).limit(limit).all()
    return users

@router.post("/approve-user/{user_id}")
@limiter.limit("10/minute")  # Rate limit: 10 approvals per minute
async def approve_user(
    request: Request,
    user_id: int,
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """موافقة المدير على مستخدم"""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if user.is_approved:
        return {"message": "User is already approved"}
    
    user.is_approved = True
    user.approved_at = datetime.utcnow()
    user.approved_by = current_admin.id
    
    db.commit()
    
    # Notify connected SSE clients
    await redis_cache.publish(f"user_approval_{user.id}", "approved")
    
    # Send email notification could go here
    
    return {"message": f"User {user.email} approved successfully"}

@router.delete("/users/{user_id}")
@limiter.limit("5/minute")  # Rate limit: 5 deletions per minute
async def delete_user(
    request: Request,
    user_id: int,
    current_admin: User = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """حذف مستخدم معين (للمدير فقط)"""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="المستخدم غير موجود")
    
    # Optional: Prevent deleting self?
    if user.id == current_admin.id:
         raise HTTPException(status_code=400, detail="لا يمكنك حذف حسابك الخاص من هنا")

    db.delete(user)
    db.commit()
    
    # Invalidate token
    from app.core.auth import invalidate_token
    await invalidate_token(user_id)
    
    return {"message": f"تم حذف المستخدم {user.email} بنجاح"}

@router.post("/refresh-data")
@limiter.limit("3/minute")  # Rate limit: 3 refreshes per minute (expensive operation)
async def refresh_stock_data(request: Request, page: int = 1, limit: int = 50, db: Session = Depends(get_db), current_admin: User = Depends(get_current_admin)):
    """إجبار النظام على تحديث البيانات من API"""
    try:
        # مسح الكاش أولاً
        await stock_cache.clear_all_cache() 
        
        # جلب بيانات جديدة من API
        api_data = await stock_cache.get_stocks(page=page, limit=limit) 
        
        if api_data and api_data.get("data"):
            return {
                "message": f"✅ تم تحديث بيانات {len(api_data['data'])} سهم",
                "stocks_updated": len(api_data["data"]), 
                "page": page,
                "limit": limit
            }
        else:
            raise HTTPException(status_code=500, detail="❌ فشل في جلب البيانات من API")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ خطأ في تحديث البيانات: {str(e)}")

@router.get("/stats")
async def get_system_stats(db: Session = Depends(get_db), current_admin: User = Depends(get_current_admin)):
    """إحصائيات النظام"""
    try:
        # جلب إحصائيات من الـ cache
        all_stocks_data = await stock_cache.get_all_stocks() 
        total_stocks = all_stocks_data.get("total", 0) 
        
        # جلب إحصائيات من قاعدة البيانات لو محتاج
        db_stats = {
            "total_stocks": total_stocks,
            "database": "PostgreSQL",
            "cache": "Redis", 
            "data_source": "TwelveData API (Profile + Quote)",
            "cache_strategy": "Redis → PostgreSQL → API"
        }
        
        return db_stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ خطأ في جلب إحصائيات النظام: {str(e)}")

@router.post("/force-api-refresh/{symbol}")
@limiter.limit("10/minute")  # Rate limit: 10 symbol refreshes per minute
async def force_api_refresh(request: Request, symbol: str, current_admin: User = Depends(get_current_admin)):
    """إجبار تحديث بيانات سهم معين من API"""
    try:
        # مسح كاش السهم المحدد
        cache_key = f"tadawul_stocks:symbol:{symbol}"
        await redis_cache.delete(cache_key)
        
        # جلب بيانات جديدة من API
        stock_data = await stock_cache.get_stock_by_symbol(symbol) 
        
        if stock_data:
            return {
                "message": f"✅ تم تحديث بيانات السهم {symbol} بنجاح",
                "symbol": symbol,
                "name": stock_data.get("name"),
                "price": stock_data.get("price")
            }
        else:
            raise HTTPException(status_code=404, detail=f"❌ لم يتم العثور على السهم {symbol}")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ خطأ في تحديث بيانات السهم: {str(e)}")