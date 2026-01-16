import time
import logging
from typing import List, Tuple, Optional, Dict, Any, Set

import mariadb
import fasttext

# ================== CONFIG ==================

MODEL_PATH = "lid.176.bin"

BATCH_LIMIT = 200
SLEEP_SEC_WHEN_EMPTY = 3

WORKER = "kk_pipeline_fasttext"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ================== SQL ==================

SQL_PICK_CHANNEL = """
SELECT m.channel_id
FROM messages m
WHERE m.analyzed = 0
  AND m.message IS NOT NULL
  AND m.message <> ''
GROUP BY m.channel_id
ORDER BY COUNT(*) DESC
LIMIT 1;
"""

SQL_FETCH_MESSAGES_BY_CHANNEL = """
SELECT id, channel_id, message
FROM messages
WHERE channel_id = ?
  AND analyzed = 0
  AND message IS NOT NULL
  AND message <> ''
ORDER BY date, id
LIMIT ?;
"""

SQL_UPDATE_MESSAGE = """
UPDATE messages
SET kazakh_ratio = ?,
    analyzed = 1
WHERE id = ?
  AND channel_id = ?;
"""

SQL_UPDATE_CHANNEL_AGG = """
UPDATE channels c
JOIN (
  SELECT channel_id,
         AVG(kazakh_ratio) AS avg_ratio,
         COUNT(*) AS cnt
  FROM messages
  WHERE channel_id = ?
    AND analyzed = 1
    AND message IS NOT NULL
    AND message <> ''
  GROUP BY channel_id
) m ON m.channel_id = c.channel_id
SET c.kk_ratio_by_f = m.avg_ratio,
    c.msgs_rcvd = m.cnt
WHERE c.channel_id = ?;
"""

# ================== DB helpers ==================

def db_connect() -> mariadb.Connection:
    return mariadb.connect(**DB_CONFIG)

def pick_next_channel(conn: mariadb.Connection) -> Optional[int]:
    cur = conn.cursor()
    cur.execute(SQL_PICK_CHANNEL)
    row = cur.fetchone()
    return int(row[0]) if row else None

def fetch_messages_for_channel(
    conn: mariadb.Connection,
    channel_id: int,
    limit: int
) -> List[Tuple[int, int, str]]:
    cur = conn.cursor()
    cur.execute(SQL_FETCH_MESSAGES_BY_CHANNEL, (channel_id, limit))
    rows = cur.fetchall()
    # rows: [(id, channel_id, message), ...]
    return [(int(r[0]), int(r[1]), r[2] or "") for r in rows]

def update_messages_batch(
    conn: mariadb.Connection,
    updates: List[Tuple[int, int, int]]
) -> None:
    """
    updates: list of (kazakh_ratio, id, channel_id)
    """
    cur = conn.cursor()
    cur.executemany(SQL_UPDATE_MESSAGE, updates)

def update_channel_aggregate(conn: mariadb.Connection, channel_id: int) -> None:
    cur = conn.cursor()
    cur.execute(SQL_UPDATE_CHANNEL_AGG, (channel_id, channel_id))

# ================== FastText analysis ==================

def fasttext_kazakh_ratio(model: fasttext.FastText._FastText, text: str) -> int:
    """
    Returns int 0..100.
    Strategy: take P(kk) from model output, convert to percent.
    """
    text = (text or "").strip()
    if not text:
        return 0

    labels, probs = model.predict(text.replace("\n", " "), k=10)
    # labels like '__label__kk'
    kk_prob = 0.0
    for lab, pr in zip(labels, probs):
        if lab.endswith("kk"):
            kk_prob = float(pr)
            break

    ratio = int(round(kk_prob * 100))
    if ratio < 0:
        return 0
    if ratio > 100:
        return 100
    return ratio

# ================== Main loop ==================

def main() -> None:
    logging.info("[%s] starting", WORKER)

    model = fasttext.load_model(MODEL_PATH)
    conn = db_connect()

    try:
        while True:
            channel_id = pick_next_channel(conn)
            if channel_id is None:
                conn.commit()  # на всякий случай
                logging.info("[%s] no work; sleep %ss", WORKER, SLEEP_SEC_WHEN_EMPTY)
                time.sleep(SLEEP_SEC_WHEN_EMPTY)
                continue

            rows = fetch_messages_for_channel(conn, channel_id, BATCH_LIMIT)
            if not rows:
                # Канал мог “опустеть” между шагами — просто продолжим
                conn.commit()
                continue

            # Анализ на десктопе (в памяти)
            updates: List[Tuple[int, int, int]] = []
            for msg_id, ch_id, text in rows:
                ratio = fasttext_kazakh_ratio(model, text)
                updates.append((ratio, msg_id, ch_id))

            # Запись в БД одним батчем + агрегация
            try:
                update_messages_batch(conn, updates)
                update_channel_aggregate(conn, channel_id)
                conn.commit()

                logging.info(
                    "[%s] channel_id=%s processed=%s, updated channel aggregate",
                    WORKER, channel_id, len(updates)
                )

            except mariadb.Error as e:
                conn.rollback()
                logging.error("[%s] DB error; rollback: %s", WORKER, e)
                # Можно продолжать, чтобы не останавливать воркер
                time.sleep(1)

    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
