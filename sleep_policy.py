import random
import time
import sqlite3

# ====== CONFIG ======
LOCK_DB = "api_sleep_lock.db"
BASE_TG_SLEEP = 300       # 5 минут
JITTER_MAX = 120          # +0..120 секунд
CHECK_INTERVAL = 2        # Проверка каждые 2 секунды
DEFAULT_LOCAL_SLEEP = 5   # Мелкие паузы

# Идентификатор текущего воркера (должен быть уникальным для каждого процесса)
WORKER_ID = "worker_1"


# ====== DB INIT ======
def init_lock_db():
    """Создаём таблицу, если её нет, и одну запись с начальным временем 0"""
    conn = sqlite3.connect(LOCK_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS api_sleep (
            id INTEGER PRIMARY KEY,
            last_call_ts INTEGER NOT NULL,
            last_call_by TEXT
        )
    """)
    cur = conn.execute("SELECT COUNT(*) FROM api_sleep")
    if cur.fetchone()[0] == 0:
        conn.execute("INSERT INTO api_sleep (id, last_call_ts, last_call_by) VALUES (1, 0, NULL)")
    conn.commit()
    conn.close()


# ====== ГЛОБАЛЬНЫЙ SLEEP ======
def sleep_before_tg_call():
    """
    Ждём перед TG API так, чтобы:
    - между запросами проходило BASE_TG_SLEEP + джиттер
    - воркеры координировали свои обращения
    """
    while True:
        now = int(time.time())
        target_sleep = BASE_TG_SLEEP + random.randint(0, JITTER_MAX)

        conn = sqlite3.connect(LOCK_DB)
        cur = conn.cursor()

        cur.execute("SELECT last_call_ts, last_call_by FROM api_sleep WHERE id = 1")
        last_call_ts, last_call_by = cur.fetchone()
        elapsed = now - last_call_ts

        # Если прошло достаточно времени и последний запрос делал другой воркер
        if elapsed >= target_sleep and last_call_by != WORKER_ID:
            # Пытаемся атомарно обновить запись
            updated = conn.execute(
                "UPDATE api_sleep SET last_call_ts = ?, last_call_by = ? WHERE id = 1 AND last_call_ts = ?",
                (now, WORKER_ID, last_call_ts)
            ).rowcount
            conn.commit()
            conn.close()
            if updated:
                # Успешно обновили — можно делать запрос к TG API
                return
            else:
                # Кто-то другой успел обновить запись, ждём
                time.sleep(CHECK_INTERVAL)
        else:
            conn.close()
            # Ждём оставшееся время или CHECK_INTERVAL, чтобы проверить снова
            remaining = max(target_sleep - elapsed, 0)
            time.sleep(min(CHECK_INTERVAL, remaining))


# ====== ЛОКАЛЬНЫЕ SLEEP ======
def local_sleep(seconds: int = DEFAULT_LOCAL_SLEEP):
    """Простая пауза между итерациями"""
    time.sleep(seconds)
