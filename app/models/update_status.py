from sqlalchemy import Column, Integer, Date, Boolean, DateTime
from sqlalchemy.sql import func
from app.core.database import Base

class UpdateStatus(Base):
    """
    جدول لحفظ حالة التحديثات اليومية
    يضمن عدم عرض أي بيانات للمستخدم حتى تكتمل جميع الحسابات لليوم (Zero-Downtime Pipeline)
    """
    __tablename__ = "update_status"

    id = Column(Integer, primary_key=True)  # سنكتفي بصّف واحد دائمًا (id=1)
    latest_ready_date = Column(Date, nullable=False) # آخر تاريخ تم حساب كل مؤشراته بنجاح
    is_updating = Column(Boolean, default=False)     # هل يوجد تحديث يعمل حاليًا؟
    started_at = Column(DateTime(timezone=True), nullable=True) # متى بدأ آخر تحديث
    completed_at = Column(DateTime(timezone=True), nullable=True) # متى انتهى آخر تحديث

    def __repr__(self):
        return f"<UpdateStatus(id={self.id}, latest_ready_date={self.latest_ready_date}, is_updating={self.is_updating})>"
