import time
import random
import sqlite3

# ====== CONFIG ======
DB_FILE = "api_queue.db"

BASE_SLEEP = 300        # 5 минут
JITTER_MAX = 300        # +0..5 минут (итого 5–10)
CHECK_INTERVAL = 2      # как часто проверяем очередь


# ====== INIT ======
def init_db():
    """
    Инициализация БД.
    Должна быть вызвана один раз при старте системы.
    """
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS api_queue (
            id INTEGER PRIMARY KEY,
            last_worker TEXT,
            last_ts INTEGER
        )
    """)
    cur = conn.execute("SELECT COUNT(*) FROM api_queue")
    if cur.fetchone()[0] == 0:
        conn.execute(
            "INSERT INTO api_queue (id, last_worker, last_ts) VALUES (1, NULL, 0)"
        )
    conn.commit()
    conn.close()


# ====== WAIT FOR TURN ======
def sleep_before_tg_call(worker_id: str):
    """
    Блокирует код воркера, пока:
    - последний запрос сделал НЕ этот воркер
    - прошло BASE_SLEEP + JITTER времени
    """
    while True:
        now = int(time.time())

        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()

        cur.execute(
            "SELECT last_worker, last_ts FROM api_queue WHERE id = 1"
        )
        last_worker, last_ts = cur.fetchone()
        conn.close()

        # 1. Если последний был Я — жду
        if last_worker == worker_id:
            time.sleep(CHECK_INTERVAL)
            continue

        # 2. Если последний был НЕ Я — проверяю таймер
        target_sleep = BASE_SLEEP + random.randint(0, JITTER_MAX)
        elapsed = now - last_ts

        if elapsed < target_sleep:
            time.sleep(min(CHECK_INTERVAL, target_sleep - elapsed))
            continue

        # 3. Очередь моя и время прошло
        return


# ====== MARK CALL ======
def mark_tg_call(worker_id: str) -> bool:
    """
    Фиксирует факт TG API вызова.
    Возвращает True, если запись успешна.
    """
    now = int(time.time())

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    updated = cur.execute(
        """
        UPDATE api_queue
        SET last_worker = ?, last_ts = ?
        WHERE id = 1 AND last_worker != ?
        """,
        (worker_id, now, worker_id)
    ).rowcount

    conn.commit()
    conn.close()

    return updated == 1
