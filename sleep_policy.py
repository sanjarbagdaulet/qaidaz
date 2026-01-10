import sqlite3
import time

DB_FILE = "q.db"


def connect_db():
    """Возвращает соединение и курсор"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn, conn.cursor()


def get_next_worker(c):
    """Возвращает имя воркера с минимальным last_time"""
    c.execute("SELECT worker FROM queue ORDER BY last_time ASC LIMIT 1")
    row = c.fetchone()
    return row["worker"] if row else None


def update_worker_time(c, conn, worker_name):
    """Обновляет last_time для текущего воркера"""
    now = int(time.time())
    c.execute("UPDATE queue SET last_time=? WHERE worker=?", (now, worker_name))
    conn.commit()
