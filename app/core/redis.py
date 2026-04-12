import redis.asyncio as redis
import os
import json
from typing import Any, Optional, List
from app.core.config import settings

class RedisCache:
    def __init__(self):
        self.redis_client = None
        self.is_connected = False
    
    async def init_redis(self):
        """تهيئة اتصال Redis"""
        try:
            self.redis_client = redis.from_url(
                settings.REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2,
                socket_keepalive=True,
                retry_on_timeout=False,
                max_connections=50
            )

            await self.redis_client.ping()
            self.is_connected = True
            print("✅ تم الاتصال بـ Redis بنجاح")
            return True
        except Exception as e:
            print(f"❌ فشل الاتصال بـ Redis: {e}")
            self.redis_client = None
            self.is_connected = False
            return False

    async def ensure_connection(self):
        if not self.is_connected or not self.redis_client:
            return await self.init_redis()
        return True
    
    async def set(self, key: str, value: Any, expire: int = 86400) -> bool:
        if not await self.ensure_connection():
            return False
        try:
            if isinstance(value, (dict, list)):
                serialized_value = json.dumps(value, ensure_ascii=False, default=str)
            else:
                serialized_value = str(value)
            result = await self.redis_client.set(key, serialized_value, ex=expire)
            return result
        except Exception as e:
            print(f"❌ خطأ في تخزين الكاش: {e}")
            return False
    
    async def get(self, key: str) -> Optional[Any]:
        if not await self.ensure_connection():
            return None
        try:
            value = await self.redis_client.get(key)
            if value is None:
                return None
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        except Exception as e:
            print(f"❌ خطأ في جلب الكاش: {e}")
            return None
    
    async def delete(self, key: str) -> bool:
        if not await self.ensure_connection():
            return False
        try:
            result = await self.redis_client.delete(key)
            return result > 0
        except Exception as e:
            print(f"❌ خطأ في حذف الكاش: {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        if not await self.ensure_connection():
            return False
        try:
            return await self.redis_client.exists(key) == 1
        except Exception as e:
            print(f"❌ خطأ في التحقق من الكاش: {e}")
            return False
    
    async def flush_all(self) -> bool:
        if not await self.ensure_connection():
            return False
        try:
            await self.redis_client.flushall()
            print("✅ تم مسح كل الكاش")
            return True
        except Exception as e:
            print(f"❌ خطأ في مسح الكاش: {e}")
            return False

    async def keys(self, pattern: str) -> List[str]:
        if not await self.ensure_connection():
            return []  
        try:
            keys = await self.redis_client.keys(pattern)
            return keys or []  
        except Exception as e:
            print(f"❌ خطأ في جلب المفاتيح: {e}")
            return []  

    async def scan_iter(self, pattern: str):
        if not await self.ensure_connection():
            return []
        try:
            keys = []
            async for key in self.redis_client.scan_iter(match=pattern):
                keys.append(key)
            return keys
        except Exception as e:
            print(f"❌ خطأ في SCAN: {e}")
            return []

    async def publish(self, channel: str, message: str) -> int:
        if not await self.ensure_connection():
            return 0
        try:
            return await self.redis_client.publish(channel, message)
        except Exception as e:
            print(f"❌ خطأ في النشر (Publish): {e}")
            return 0

    async def pubsub(self):
        if not await self.ensure_connection():
            return None
        return self.redis_client.pubsub()

redis_cache = RedisCache()

async def store_reset_token(user_id: int, token: str, expire_minutes: int = 15):
    """تخزين توكن استعادة كلمة المرور"""
    await redis_cache.set(f"reset_token:{token}", str(user_id), expire=expire_minutes * 60)

async def get_reset_token(token: str) -> Optional[int]:
    """جلب user_id من توكن الاستعادة"""
    result = await redis_cache.get(f"reset_token:{token}")
    return int(result) if result else None

async def delete_reset_token(token: str):
    """حذف توكن الاستعادة"""
    await redis_cache.delete(f"reset_token:{token}")

async def store_verification_token(user_id: int, token: str, expire_minutes: int = 60):
    """تخزين توكن التحقق من البريد"""
    await redis_cache.set(f"verify_token:{token}", str(user_id), expire=expire_minutes * 60)

async def get_verification_token(token: str) -> Optional[int]:
    """جلب user_id من توكن التحقق"""
    result = await redis_cache.get(f"verify_token:{token}")
    return int(result) if result else None

async def delete_verification_token(token: str):
    """حذف توكن التحقق"""
    await redis_cache.delete(f"verify_token:{token}")