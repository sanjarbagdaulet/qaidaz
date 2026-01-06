import random
import time
import sqlite3

DB_PATH = "telegram_channels.db"

# ====== TG API sleep ======
BASE_TG_SLEEP = 300       # 5 минут
JITTER_MAX = 120          # +0..120 секунд
CHECK_INTERVAL = 5        # как часто проверяем last_call_ts

# ====== мелкие sleep ======
DEFAULT_LOCAL_SLEEP = 5   # секунды


def get_db_connection():
    return sqlite3.connect(DB_PATH)


# -----------------------------
# Глобальный sleep перед TG API
# -----------------------------
def sleep_before_tg_call():
    """
    Глобальный sleep перед TG API
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    while True:
        cursor.execute("SELECT last_call_ts FROM api_sleep WHERE id = 1")
        last_call_ts = cursor.fetchone()[0]
        now = int(time.time())
        target_sleep = BASE_TG_SLEEP + random.randint(0, JITTER_MAX)
        elapsed = now - last_call_ts

        if elapsed >= target_sleep:
            break

        remaining = target_sleep - elapsed
        sleep_for = min(CHECK_INTERVAL, remaining)
        time.sleep(sleep_for)

    conn.close()


def mark_tg_call():
    """
    Фиксируем момент обращения к TG API
    """
    now = int(time.time())
    conn = get_db_connection()
    conn.execute(
        "UPDATE api_sleep SET last_call_ts = ? WHERE id = 1",
        (now,)
    )
    conn.commit()
    conn.close()


# -----------------------------
# Локальные sleep / паузы между итерациями
# -----------------------------
def local_sleep(seconds: int = DEFAULT_LOCAL_SLEEP):
    """
    Простая пауза
    """
    time.sleep(seconds)


def local_sleep_jitter(base: int = DEFAULT_LOCAL_SLEEP, jitter: int = 0):
    """
    Пауза с джиттером
    """
    s = base + random.randint(0, jitter)
    time.sleep(s)
