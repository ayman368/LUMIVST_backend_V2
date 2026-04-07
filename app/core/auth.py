from datetime import datetime, timedelta
from uuid import uuid4
import hashlib
import os
import secrets

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from passlib.context import CryptContext

SECRET_KEY = os.getenv("SECRET_KEY")
if not SECRET_KEY:
    raise RuntimeError("SECRET_KEY is required and must be configured in environment variables")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "15"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "14"))

pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
from app.core.redis import redis_cache

security = HTTPBearer(auto_error=False)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

def generate_token() -> str:
    """Generate a crypgraphically secure random token (32 bytes = 64 hex chars)."""
    return secrets.token_hex(32)

def hash_token(token: str) -> str:
    """Hash a token using SHA-256."""
    return hashlib.sha256(token.encode()).hexdigest()

def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update(
        {
            "exp": expire,
            "iat": datetime.utcnow(),
            "jti": str(uuid4()),
            "type": "access",
        }
    )
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def create_refresh_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update(
        {
            "exp": expire,
            "iat": datetime.utcnow(),
            "jti": str(uuid4()),
            "type": "refresh",
        }
    )
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def store_token_in_redis(user_id: int, token: str):
    try:
        payload = decode_token(token)
        jti = payload.get("jti")
        if not jti:
            raise ValueError("Missing jti in access token")

        success = await redis_cache.set(
            f"access_token:{jti}",
            str(user_id),
            expire=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        )
        if success:
            await redis_cache.set(
                f"access_jti:{user_id}:{jti}",
                "1",
                expire=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            )
        else:
            print("⚠️ Failed to store token in Redis")
    except Exception as e:
        print(f"⚠️ Warning: Could not store token in Redis: {e}")


async def store_refresh_token_in_redis(user_id: int, token: str):
    try:
        payload = decode_token(token)
        jti = payload.get("jti")
        if not jti:
            raise ValueError("Missing jti in refresh token")

        success = await redis_cache.set(
            f"refresh_token:{jti}",
            str(user_id),
            expire=REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60,
        )
        if success:
            await redis_cache.set(
                f"refresh_jti:{user_id}:{jti}",
                "1",
                expire=REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60,
            )
    except Exception as e:
        print(f"⚠️ Warning: Could not store refresh token in Redis: {e}")


async def invalidate_token(user_id: int, token_jti: str | None = None):
    try:
        if token_jti:
            await redis_cache.delete(f"access_token:{token_jti}")
            await redis_cache.delete(f"access_jti:{user_id}:{token_jti}")
            return

        keys = await redis_cache.scan_iter(f"access_jti:{user_id}:*")
        for key in keys:
            jti = key.split(":")[-1]
            await redis_cache.delete(f"access_token:{jti}")
            await redis_cache.delete(key)
    except Exception as e:
        print(f"⚠️ Warning: Could not invalidate token in Redis: {e}")


async def invalidate_refresh_token(user_id: int, token_jti: str | None = None):
    try:
        if token_jti:
            await redis_cache.delete(f"refresh_token:{token_jti}")
            await redis_cache.delete(f"refresh_jti:{user_id}:{token_jti}")
            return

        keys = await redis_cache.scan_iter(f"refresh_jti:{user_id}:*")
        for key in keys:
            jti = key.split(":")[-1]
            await redis_cache.delete(f"refresh_token:{jti}")
            await redis_cache.delete(key)
    except Exception as e:
        print(f"⚠️ Warning: Could not invalidate refresh token in Redis: {e}")


async def verify_token_exists(user_id: int, token: str) -> bool:
    try:
        payload = decode_token(token)
        jti = payload.get("jti")
        token_type = payload.get("type")
        if not jti or token_type != "access":
            return False

        if not redis_cache.is_connected and not await redis_cache.ensure_connection():
            return False

        stored_user_id = await redis_cache.get(f"access_token:{jti}")
        return stored_user_id == str(user_id)
    except Exception:
        return False


async def verify_refresh_token_exists(user_id: int, token: str) -> bool:
    try:
        payload = decode_token(token)
        jti = payload.get("jti")
        token_type = payload.get("type")
        if not jti or token_type != "refresh":
            return False

        if not redis_cache.is_connected and not await redis_cache.ensure_connection():
            return False

        stored_user_id = await redis_cache.get(f"refresh_token:{jti}")
        return stored_user_id == str(user_id)
    except Exception:
        return False

def decode_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="توكن غير صالح",
            headers={"WWW-Authenticate": "Bearer"},
        )

async def verify_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
):
    token = None
    if credentials and credentials.credentials:
        token = credentials.credentials
    else:
        token = request.cookies.get("session_token")

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication token missing",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = decode_token(token)
    user_id = int(payload.get("sub"))

    if not await verify_token_exists(user_id, token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="توكن غير صالح أو منتهي",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return token