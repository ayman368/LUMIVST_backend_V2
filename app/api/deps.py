from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.auth import verify_token, decode_token
from app.models.user import User

def get_current_user(token: str = Depends(verify_token), db: Session = Depends(get_db)) -> User:
    payload = decode_token(token)
    user_id = int(payload.get("sub"))
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if not user.is_approved:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="Account pending admin approval"
        )
    
    return user

def get_current_admin(current_user: User = Depends(get_current_user)) -> User:
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="The user doesn't have enough privileges"
        )
    return current_user
