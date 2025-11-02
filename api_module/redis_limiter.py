import time
import math
from typing import Optional
from loguru import logger
import redis


class RedisWindowLimiter:
    """
    초/분 등 고정 윈도우 전역 레이트 리미터.
    여러 워커/컨테이너가 동일 Redis를 쓰면 전역 카운팅이 됨.
    """

    def __init__(
        self,
        redis_url: Optional[str],
        prefix: str = "rate",
        max_rps: int = 0,
        max_rpm: int = 0,
        sec_ttl: int = 2,
        min_ttl: int = 120,
        connect_timeout: float = 1.5,
        socket_timeout: float = 1.5,
    ):
        self.prefix = prefix
        self.max_rps = max(0, int(max_rps))
        self.max_rpm = max(0, int(max_rpm))
        self.sec_ttl = sec_ttl
        self.min_ttl = min_ttl
        self._redis: Optional[redis.Redis] = None

        if redis_url:
            try:
                self._redis = redis.Redis.from_url(
                    redis_url,
                    socket_connect_timeout=connect_timeout,
                    socket_timeout=socket_timeout,
                )
                self._redis.ping()
            except Exception as e:
                logger.warning("Redis disabled in limiter: {}", e)
                self._redis = None

    def _incr_with_ttl(self, key: str, limit: int, ttl: int) -> bool:
        if not self._redis or limit <= 0:
            return True
        pipe = self._redis.pipeline()
        try:
            pipe.incr(key)
            pipe.expire(key, ttl)
            cur, _ = pipe.execute()
            cur = int(cur or 0)
            if cur > limit:
                try:
                    self._redis.decr(key)
                except Exception:
                    pass
                return False
            return True
        except Exception:
            return True

    def acquire(self) -> None:
        """
        전역 한도(초/분)를 동시에 만족할 때까지 대기.
        Redis가 없거나 한도가 0이면 no-op.
        """
        if not self._redis or (self.max_rps == 0 and self.max_rpm == 0):
            return

        while True:
            now = time.time()
            ok_sec = True
            ok_min = True

            if self.max_rps > 0:
                sec_key = f"{self.prefix}:sec:{int(now)}"
                ok_sec = self._incr_with_ttl(sec_key, self.max_rps, self.sec_ttl)

            if self.max_rpm > 0:
                min_key = f"{self.prefix}:min:{int(now // 60)}"
                ok_min = self._incr_with_ttl(min_key, self.max_rpm, self.min_ttl)

            if ok_sec and ok_min:
                return

            now = time.time()
            sleep_sec = 0.0
            if not ok_sec and self.max_rps > 0:
                sleep_sec = max(sleep_sec, 1.0 - (now - math.floor(now)))
            if not ok_min and self.max_rpm > 0:
                sec_into_min = int(now) % 60
                sleep_sec = max(sleep_sec, 60 - sec_into_min + 0.01)
            time.sleep(min(sleep_sec + 0.01, 1.2))
