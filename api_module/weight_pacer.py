import time
import math
import random
from typing import Dict


def update_speed_by_weight(
    headers: Dict[str, str], weight_limit_1m: int, slowdown_ratio: float
) -> None:
    used_raw = headers.get("X-MBX-USED-WEIGHT-1M") or headers.get("X-MBX-USED-WEIGHT")
    if used_raw is None:
        return

    used_str = str(used_raw).strip()
    if not used_str.isdigit():
        return

    if weight_limit_1m <= 0 or not (0.0 <= slowdown_ratio <= 1.0):
        return

    used = int(used_str)
    if used >= int(weight_limit_1m * slowdown_ratio):
        time.sleep(random.randint(400, 900) / 1000.0)


def pace_next_request_by_used_weight(
    headers: Dict[str, str], weight_limit_1m: int
) -> None:
    """
    남은 1분 동안 남은 weight를 균등 분할하도록 sleep.
    헤더가 없으면 아무 것도 하지 않음.
    """
    used_raw = headers.get("X-MBX-USED-WEIGHT-1M") or headers.get("X-MBX-USED-WEIGHT")
    if not used_raw:
        return
    try:
        used = int(str(used_raw).strip())
        remain = max(1, int(weight_limit_1m) - used)
        now = time.time()
        sec_left = max(1.0, 60.0 - (now - math.floor(now / 60) * 60))
        target_interval = sec_left / float(remain)
        sleep_for = min(max(target_interval, 0.0), 1.5) + random.uniform(0.0, 0.05)
        time.sleep(sleep_for)
    except Exception:
        pass
