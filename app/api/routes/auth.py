
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.auth import *
from app.api.deps import get_current_user as require_current_user
from app.models.user import User
from app.schemas.auth import *
from app.core.redis import redis_cache, store_verification_token, get_verification_token, delete_verification_token
from app.services.email_service import send_email
from app.core.config import settings
import uuid
import logging
import re
import secrets
import hashlib
import base64
import asyncio
from urllib.parse import urlencode
from app.core.limiter import limiter

router = APIRouter(prefix="/auth", tags=["authentication"])
logger = logging.getLogger(__name__)


def validate_password_strength(password: str):
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="كلمة المرور يجب أن تكون 8 أحرف على الأقل")
    if not re.search(r"[A-Z]", password):
        raise HTTPException(status_code=400, detail="كلمة المرور يجب أن تحتوي على حرف كبير واحد على الأقل")
    if not re.search(r"[a-z]", password):
        raise HTTPException(status_code=400, detail="كلمة المرور يجب أن تحتوي على حرف صغير واحد على الأقل")
    if not re.search(r"\d", password):
        raise HTTPException(status_code=400, detail="كلمة المرور يجب أن تحتوي على رقم واحد على الأقل")
    if not re.search(r"[!@#$%^&*(),.?\":{}|<>]", password):
        raise HTTPException(status_code=400, detail="كلمة المرور يجب أن تحتوي على رمز خاص واحد على الأقل")


def set_auth_cookies(response: Response, access_token: str, refresh_token: str):
    is_secure = settings.ENVIRONMENT.lower() == "production"
    cookie_samesite = "none" if is_secure else "lax"
    response.set_cookie(
        key="session_token",
        value=access_token,
        httponly=True,
        secure=is_secure,
        samesite=cookie_samesite,
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        path="/",
    )
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=is_secure,
        samesite=cookie_samesite,
        max_age=REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60,
        path="/",
    )


def clear_auth_cookies(response: Response):
    response.delete_cookie("session_token", path="/")
    response.delete_cookie("refresh_token", path="/")
    response.delete_cookie("pending_token", path="/")


def set_pending_cookie(response: Response, pending_token: str):
    is_secure = settings.ENVIRONMENT.lower() == "production"
    cookie_samesite = "none" if is_secure else "lax"
    response.set_cookie(
        key="pending_token",
        value=pending_token,
        httponly=True,
        secure=is_secure,
        samesite=cookie_samesite,
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        path="/",
    )


async def create_and_store_tokens(user: User):
    access_token = create_access_token(
        data={"sub": str(user.id), "email": user.email, "is_approved": user.is_approved, "is_admin": user.is_admin}
    )
    refresh_token = create_refresh_token(data={"sub": str(user.id), "email": user.email})
    await store_token_in_redis(user.id, access_token)
    await store_refresh_token_in_redis(user.id, refresh_token)
    return access_token, refresh_token


def _pkce_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")


async def create_oauth_state(provider: str):
    state = secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(64)
    await redis_cache.set(f"oauth_state:{provider}:{state}", verifier, expire=600)
    return state, verifier


async def consume_oauth_state(provider: str, state: str):
    key = f"oauth_state:{provider}:{state}"
    verifier = await redis_cache.get(key)
    if verifier:
        await redis_cache.delete(key)
    return verifier

@router.post("/register", status_code=status.HTTP_201_CREATED)
async def register(user: UserRegister, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    # التحقق من وجود المستخدم
    db_user = db.query(User).filter(User.email == user.email).first()
    if db_user:
        raise HTTPException(status_code=400, detail="البريد الإلكتروني مسجل بالفعل")
    
    validate_password_strength(user.password)
    
    
    # إنشاء المستخدم
    hashed_password = get_password_hash(user.password)
    db_user = User(
        email=user.email, 
        hashed_password=hashed_password, 
        full_name=user.full_name,
        is_verified=False, # Ensure user is not verified initially
        is_approved=False  # Must be approved by admin
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    
    # Send Verification Email
    try:
        verification_token = str(uuid.uuid4())
        await store_verification_token(db_user.id, verification_token)
        verification_link = f"http://localhost:3000/auth/verify-email?token={verification_token}"
        
        email_body = f"""
        <h1>مرحباً {db_user.full_name}</h1>
        <p>شكراً لتسجيلك في LUMIVST. يرجى تأكيد بريدك الإلكتروني بالضغط على الرابط أدناه:</p>
        <a href="{verification_link}" style="background-color: #2563EB; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">تأكيد البريد الإلكتروني</a>
        <p>أو انسخ الرابط التالي:</p>
        <p>{verification_link}</p>
        """
        
        background_tasks.add_task(send_email, db_user.email, "تأكيد البريد الإلكتروني - LUMIVST", email_body)
    except Exception as e:
        print(f"⚠️ Failed to queue verification email: {e}")
    
    return {"message": "تم إنشاء الحساب بنجاح. الحساب بانتظار موافقة الإدارة وتأكيد البريد الإلكتروني."}

@router.post("/refresh-token")
async def refresh_token(request: Request, response: Response, db: Session = Depends(get_db)):
    refresh_token = request.cookies.get("refresh_token")
    user_id = None
    token_jti = None

    if refresh_token:
        payload = decode_token(refresh_token)
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=401, detail="Invalid refresh token type")

        user_id = int(payload.get("sub"))
        token_jti = payload.get("jti")
        if not await verify_refresh_token_exists(user_id, refresh_token):
            raise HTTPException(status_code=401, detail="Refresh token expired or revoked")
    else:
        # Backward-compatible fallback for pending-approval flow using bearer access token.
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Refresh token missing")
        access_token = auth_header.split(" ", 1)[1].strip()
        payload = decode_token(access_token)
        user_id = int(payload.get("sub"))
        if not await verify_token_exists(user_id, access_token):
            raise HTTPException(status_code=401, detail="Access token expired or revoked")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if not user.is_approved:
        raise HTTPException(status_code=403, detail="Account pending admin approval")

    if token_jti:
        await invalidate_refresh_token(user_id, token_jti)
    access_token, new_refresh_token = await create_and_store_tokens(user)
    set_auth_cookies(response, access_token, new_refresh_token)

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "email": user.email,
            "full_name": user.full_name,
            "is_verified": user.is_verified,
            "is_approved": user.is_approved,
            "is_admin": user.is_admin,
        },
    }

@router.post("/login")
@limiter.limit("5/minute")
async def login(request: Request, response: Response, user: UserLogin, db: Session = Depends(get_db)):
    # Rate limiting handled by decorator
    
    try:
        db_user = db.query(User).filter(User.email == user.email).first()
        if not db_user:
            raise HTTPException(status_code=401, detail="بيانات الدخول غير صحيحة")
            
        # Check Account Lockout (Optional Implementation)
        if db_user.is_locked:
             raise HTTPException(status_code=403, detail="تم قفل الحساب. يرجى الاتصال بالدعم.")
        
        if not verify_password(user.password, db_user.hashed_password):
            raise HTTPException(status_code=401, detail="بيانات الدخول غير صحيحة")
            
        # ✅ Check Approval Status
        if not db_user.is_approved:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, 
                detail="الحساب بانتظار موافقة الإدارة. سيتم إشعارك عند التفعيل."
            )
        
        access_token, refresh_token = await create_and_store_tokens(db_user)
        set_auth_cookies(response, access_token, refresh_token)
        
        return {
            "access_token": access_token, 
            "token_type": "bearer",
            "user": {
                "id": db_user.id,
                "email": db_user.email,
                "full_name": db_user.full_name,
                "is_verified": db_user.is_verified,
                "is_approved": db_user.is_approved,
                "is_admin": db_user.is_admin
            }
        }
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected login error")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    token: str = Depends(verify_token),
    current_user: User = Depends(require_current_user),
):
    payload = decode_token(token)
    access_jti = payload.get("jti")
    if access_jti:
        await invalidate_token(current_user.id, access_jti)
    else:
        await invalidate_token(current_user.id)

    refresh_token = request.cookies.get("refresh_token")
    if refresh_token:
        refresh_payload = decode_token(refresh_token)
        refresh_jti = refresh_payload.get("jti")
        if refresh_jti:
            await invalidate_refresh_token(current_user.id, refresh_jti)
    clear_auth_cookies(response)
    return {"message": "تم تسجيل الخروج بنجاح"}

@router.get("/me", response_model=UserResponse)
async def get_current_user(token: str = Depends(verify_token), db: Session = Depends(get_db)):
    # verify_token returns the token string after validation
    payload = decode_token(token)
    user_id = int(payload.get("sub"))
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="المستخدم غير موجود")
    
    return user

@router.put("/profile", response_model=UserResponse)
async def update_profile(
    user_update: UserUpdate,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    db_user = db.query(User).filter(User.id == current_user.id).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="المستخدم غير موجود")
        
    # Update fields
    if user_update.full_name:
        db_user.full_name = user_update.full_name
    
    if user_update.email and user_update.email != db_user.email:
        # Require current password for email change
        if not user_update.current_password:
            raise HTTPException(status_code=400, detail="يجب إدخال كلمة المرور الحالية لتغيير البريد الإلكتروني")
        if not verify_password(user_update.current_password, db_user.hashed_password):
            raise HTTPException(status_code=400, detail="كلمة المرور الحالية غير صحيحة")

        # Check if email is taken by another user
        existing_user = db.query(User).filter(User.email == user_update.email).first()
        if existing_user and existing_user.id != current_user.id:
            raise HTTPException(status_code=400, detail="البريد الإلكتروني مستخدم بالفعل")
        db_user.email = user_update.email
        db_user.is_verified = False # Reset verification status if email changes
    
    # Only update password if provided and not empty
    if user_update.password:
        if len(user_update.password) < 8:
            raise HTTPException(status_code=400, detail="كلمة المرور يجب أن تكون 8 أحرف على الأقل")
        db_user.hashed_password = get_password_hash(user_update.password)
        
    db.commit()
    db.refresh(db_user)
    
    return db_user

@router.delete("/delete-account")
async def delete_account(
    response: Response,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    db_user = db.query(User).filter(User.id == current_user.id).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="المستخدم غير موجود")
    
    # Delete user from database
    db.delete(db_user)
    db.commit()
    
    # Invalidate token
    await invalidate_token(current_user.id)
    await invalidate_refresh_token(current_user.id)
    clear_auth_cookies(response)
    
    return {"message": "تم حذف الحساب بنجاح"}

@router.post("/forget-password")
async def forget_password(request: ForgetPasswordRequest, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == request.email).first()
    
    # SECURITY: Always return success to prevent Email Enumeration
    if not user:
        return {"message": "إذا كان البريد مسجلاً، سيتم إرسال رابط الاستعادة."}
    
    # 1. Generate Secure Token (Raw)
    raw_token = generate_token()
    
    # 2. Hash it for storage
    token_hash = hash_token(raw_token)
    
    # 3. Save Hash + Expiry to DB
    user.reset_token_hash = token_hash
    user.reset_token_expires_at = datetime.utcnow() + timedelta(minutes=15)
    db.commit()
    
    # 4. Construct Link with Raw Token
    reset_link = f"{settings.FRONTEND_URL}/auth/reset-password?token={raw_token}"
    
    email_body = f"""
    <h1>استعادة كلمة المرور</h1>
    <p>لقد طلبت استعادة كلمة المرور لحسابك في LUMIVST.</p>
    <p>اضغط على الرابط أدناه لتعيين كلمة مرور جديدة:</p>
    <a href="{reset_link}" style="background-color: #2563EB; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">تغيير كلمة المرور</a>
    <p>أو انسخ الرابط التالي:</p>
    <p>{reset_link}</p>
    <p>هذا الرابط صالح لمدة 15 دقيقة.</p>
    """
    
    background_tasks.add_task(send_email, user.email, "استعادة كلمة المرور - LUMIVST", email_body)
    
    return {"message": "إذا كان البريد مسجلاً، سيتم إرسال رابط الاستعادة."}

@router.post("/reset-password")
async def reset_password(request: ResetPasswordRequest, db: Session = Depends(get_db)):
    validate_password_strength(request.password)

    # 1. Hash incoming token
    incoming_token_hash = hash_token(request.token)
    
    # 2. Find user by Token Hash
    user = db.query(User).filter(User.reset_token_hash == incoming_token_hash).first()
    
    if not user:
        raise HTTPException(status_code=400, detail="الرابط غير صالح أو منتهي")
        
    # 3. Check Expiry
    if not user.reset_token_expires_at or user.reset_token_expires_at < datetime.utcnow():
        raise HTTPException(status_code=400, detail="انتهت صلاحية الرابط")
        
    # 4. Update Password
    user.hashed_password = get_password_hash(request.password)
    
    # 5. SECURITY: Invalidate Token (Single Use)
    user.reset_token_hash = None
    user.reset_token_expires_at = None
    
    db.commit()
    
    return {"message": "تم تغيير كلمة المرور بنجاح"}


@router.get("/pending-status/stream")
async def pending_status_stream(request: Request, db: Session = Depends(get_db)):
    pending_token = request.cookies.get("pending_token")
    if not pending_token:
        raise HTTPException(status_code=401, detail="Pending token missing")

    payload = decode_token(pending_token)
    if payload.get("scope") != "check_approval_only":
        raise HTTPException(status_code=401, detail="Invalid pending token scope")

    user_id = int(payload.get("sub"))
    if not await verify_token_exists(user_id, pending_token):
        raise HTTPException(status_code=401, detail="Pending token expired")

    async def event_generator():
        while True:
            user = db.query(User).filter(User.id == user_id).first()
            approved = bool(user and user.is_approved)
            yield f"data: {{\"approved\": {str(approved).lower()}}}\n\n"
            if approved:
                break
            await asyncio.sleep(10)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@router.get("/verify-email")
async def verify_email(token: str, db: Session = Depends(get_db)):
    user_id = await get_verification_token(token)
    if not user_id:
        raise HTTPException(status_code=400, detail="توكن غير صالح أو منتهي")
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="المستخدم غير موجود")
        
    user.is_verified = True
    db.commit()
    
    await delete_verification_token(token)
    return {"message": "تم التحقق من البريد الإلكتروني بنجاح"}

# Social Login - Google
import httpx

@router.get("/google/login")
async def google_login():
    redirect_uri = f"{settings.FRONTEND_URL}/auth/callback/google"
    state, verifier = await create_oauth_state("google")
    challenge = _pkce_challenge(verifier)
    params = {
        "response_type": "code",
        "client_id": settings.GOOGLE_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "scope": "openid email profile",
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    return {
        "url": f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"
    }

@router.post("/google/callback")
async def google_callback(code: str, state: str, response: Response, db: Session = Depends(get_db)):
    try:
        verifier = await consume_oauth_state("google", state)
        if not verifier:
            raise HTTPException(status_code=400, detail="Invalid OAuth state")

        # Check if Google credentials are configured
        if not settings.GOOGLE_CLIENT_ID or not settings.GOOGLE_CLIENT_SECRET:
            raise HTTPException(status_code=500, detail="Google OAuth configuration is missing")
            
        token_url = "https://oauth2.googleapis.com/token"
        redirect_uri = f"{settings.FRONTEND_URL}/auth/callback/google"
        data = {
            "code": code,
            "client_id": settings.GOOGLE_CLIENT_ID,
            "client_secret": settings.GOOGLE_CLIENT_SECRET,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
            "code_verifier": verifier,
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(token_url, data=data)
            token_data = response.json()
            
        if "error" in token_data:
            error_msg = token_data.get("error_description", token_data.get("error", "Google Login Failed"))
            raise HTTPException(status_code=400, detail=error_msg)
            
        id_token = token_data.get("id_token")
        access_token_from_google = token_data.get("access_token")
        
        if not access_token_from_google:
            raise HTTPException(status_code=400, detail="لم يتم الحصول على رمز الولوج من Google")
        
        # Get user info
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(f"https://www.googleapis.com/oauth2/v3/userinfo?access_token={access_token_from_google}")
            user_info = response.json()
            
        if response.status_code != 200:
            error_msg = user_info.get("error_description", "Failed to get user info from Google")
            raise HTTPException(status_code=400, detail=error_msg)
        
        email = user_info.get("email")
        name = user_info.get("name")
        
        if not email:
            raise HTTPException(status_code=400, detail="لم تقدم Google بريدك الإلكتروني")
        
        # Find or create user
        user = db.query(User).filter(User.email == email).first()
        if not user:
            # Create new user
            # Generate random password
            random_password = str(uuid.uuid4())
            hashed_password = get_password_hash(random_password)
            user = User(
                email=email,
                hashed_password=hashed_password,
                full_name=name,
                is_verified=True, # Email verified by Google
                is_approved=False # Must be approved
            )
            db.add(user)
            db.commit()
            db.refresh(user)
        # If the account is not yet approved by an admin, return 403 with user info
        # Frontend will show pending approval page and poll for approval
        if not user.is_approved:
            # Create a temporary JWT just for checking approval status (not for API access)
            # This is limited-scope token to verify approval status
            temp_token = create_access_token(data={
                "sub": str(user.id), 
                "email": user.email,
                "is_approved": False,
                "is_admin": False,
                "scope": "check_approval_only"
            })
            await store_token_in_redis(user.id, temp_token)
            # Return 403 but with user info so frontend can poll for approval
            pending_response = JSONResponse(
                status_code=403,
                content={
                    "detail": "الحساب بانتظار موافقة الإدارة. سيتم إشعارك عند التفعيل.",
                    "user": {
                        "id": user.id,
                        "email": user.email,
                        "full_name": user.full_name,
                        "is_verified": user.is_verified,
                        "is_approved": user.is_approved,
                        "is_admin": user.is_admin
                    },
                    "temp_token": temp_token  # Limited token for checking status
                }
            )
            set_pending_cookie(pending_response, temp_token)
            return pending_response
            

        # Create JWT for approved users
        access_token, refresh_token = await create_and_store_tokens(user)
        set_auth_cookies(response, access_token, refresh_token)

        return {
            "access_token": access_token,
            "token_type": "bearer",
            "user": {
                "id": user.id,
                "email": user.email,
                "full_name": user.full_name,
                "is_verified": user.is_verified,
                "is_approved": user.is_approved,
                "is_admin": user.is_admin
            }
        }
    except HTTPException:
        raise
    except Exception:
        logger.exception("Google callback failed")
        raise HTTPException(status_code=500, detail="Google login failed")

# Social Login - Facebook
@router.get("/facebook/login")
async def facebook_login():
    redirect_uri = f"{settings.FRONTEND_URL}/auth/callback/facebook"
    state, _ = await create_oauth_state("facebook")
    params = {
        "client_id": settings.FACEBOOK_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "scope": "public_profile,email",
        "state": state,
    }
    return {
        "url": f"https://www.facebook.com/v18.0/dialog/oauth?{urlencode(params)}"
    }

@router.post("/facebook/callback")
async def facebook_callback(code: str, state: str, response: Response, db: Session = Depends(get_db)):
    try:
        verifier = await consume_oauth_state("facebook", state)
        if not verifier:
            raise HTTPException(status_code=400, detail="Invalid OAuth state")

        # Check if Facebook credentials are configured
        if not settings.FACEBOOK_CLIENT_ID or not settings.FACEBOOK_CLIENT_SECRET:
            raise HTTPException(status_code=500, detail="Facebook OAuth configuration is missing")
            
        token_url = "https://graph.facebook.com/v18.0/oauth/access_token"
        redirect_uri = f"{settings.FRONTEND_URL}/auth/callback/facebook"
        params = {
            "client_id": settings.FACEBOOK_CLIENT_ID,
            "client_secret": settings.FACEBOOK_CLIENT_SECRET,
            "redirect_uri": redirect_uri,
            "code": code,
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(token_url, params=params)
            token_data = response.json()
            
        if "error" in token_data:
            error_msg = token_data.get("error", {}).get("message", "Facebook Login Failed")
            raise HTTPException(status_code=400, detail=error_msg)
            
        access_token_from_facebook = token_data.get("access_token")
        
        if not access_token_from_facebook:
            raise HTTPException(status_code=400, detail="لم يتم الحصول على رمز الولوج من Facebook")
        
        # Get user info
        async with httpx.AsyncClient() as client:
            response = await client.get(f"https://graph.facebook.com/me?fields=id,name,email&access_token={access_token_from_facebook}")
            user_info = response.json()
            
        if "error" in user_info:
            error_msg = user_info.get("error", {}).get("message", "Failed to get user info from Facebook")
            raise HTTPException(status_code=400, detail=error_msg)
        
        email = user_info.get("email")
        name = user_info.get("name")
        
        if not email:
            raise HTTPException(status_code=400, detail="لم تقدم Facebook بريدك الإلكتروني")
        
        # Find or create user
        user = db.query(User).filter(User.email == email).first()
        if not user:
            # Create new user
            random_password = str(uuid.uuid4())
            hashed_password = get_password_hash(random_password)
            user = User(
                email=email,
                hashed_password=hashed_password,
                full_name=name,
                is_verified=True,  # Email verified by Facebook
                is_approved=False  # Must be approved
            )
            db.add(user)
            db.commit()
            db.refresh(user)
        
        # If the account is not yet approved by an admin, return 403 with user info
        # Frontend will show pending approval page and poll for approval
        if not user.is_approved:
            # Create a temporary JWT just for checking approval status (not for API access)
            # This is limited-scope token to verify approval status
            temp_token = create_access_token(data={
                "sub": str(user.id), 
                "email": user.email,
                "is_approved": False,
                "is_admin": False,
                "scope": "check_approval_only"
            })
            await store_token_in_redis(user.id, temp_token)
            # Return 403 but with user info so frontend can poll for approval
            pending_response = JSONResponse(
                status_code=403,
                content={
                    "detail": "الحساب بانتظار موافقة الإدارة. سيتم إشعارك عند التفعيل.",
                    "user": {
                        "id": user.id,
                        "email": user.email,
                        "full_name": user.full_name,
                        "is_verified": user.is_verified,
                        "is_approved": user.is_approved,
                        "is_admin": user.is_admin
                    },
                    "temp_token": temp_token  # Limited token for checking status
                }
            )
            set_pending_cookie(pending_response, temp_token)
            return pending_response

        # Create JWT for approved users
        jwt_token, refresh_token = await create_and_store_tokens(user)
        set_auth_cookies(response, jwt_token, refresh_token)

        return {
            "access_token": jwt_token,
            "token_type": "bearer",
            "user": {
                "id": user.id,
                "email": user.email,
                "full_name": user.full_name,
                "is_verified": user.is_verified,
                "is_approved": user.is_approved,
                "is_admin": user.is_admin
            }
        }
    except HTTPException:
        raise
    except Exception:
        logger.exception("Facebook callback failed")
        raise HTTPException(status_code=500, detail="Facebook login failed")