from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.sql import func
from app.core.database import Base

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    full_name = Column(String)
    is_verified = Column(Boolean, default=False)
    is_admin = Column(Boolean, default=False)
    
    # Secure Password Reset Fields
    reset_token_hash = Column(String, nullable=True, index=True)
    reset_token_expires_at = Column(DateTime, nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Approval System
    is_approved = Column(Boolean, default=False)
    approved_at = Column(DateTime, nullable=True)
    approved_by = Column(Integer, nullable=True)
    
    # Security Enhancements
    is_locked = Column(Boolean, default=False)
    failed_login_attempts = Column(Integer, default=0)
    approval_token = Column(String, unique=True, nullable=True)