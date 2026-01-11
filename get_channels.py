from telethon import TelegramClient
from accounts import ACC_MAIN
from telethon.errors import FloodWaitError
from telethon.tl.functions.channels import GetChannelRecommendationsRequest
from telethon.tl.types import InputPeerChannel

import time
import sqlite3
import random
import logging


# ---------------- CONFIG ----------------

WORKER = "get_channels"
INTERVAL = 2
DB_PATH = "telegram_client.db"

MIN_SUBSCRIBERS = 100_000
BASE = 300
JITTER_MAX = 300

a = ACC_MAIN
client = TelegramClient(a.session + WORKER, a.api_id, a.api_hash)

logging.basicConfig(
    format='[%(levelname)s %(asctime)s] %(name)s: %(message)s',
    level=logging.ERROR,
    filename=WORKER + '.log',
)


# ---------------- DB ----------------

def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


def get_channel(conn):
    row = conn.execute(
        """
        SELECT tg_id, access_hash
        FROM channels
        WHERE subscribers_count >= ?
          AND c_rcvd = 0
          AND (
                kk_ratio_by_l >= 0.7
                OR kk_ratio_by_f >= 0.7
              )
        ORDER BY subscribers_count DESC
        LIMIT 1;
        """,
        (MIN_SUBSCRIBERS,)
    ).fetchone()

    if row:
        return row["tg_id"], row["access_hash"]

    return None, None


def save_channel(conn, ch):
    conn.execute(
        """
        INSERT INTO channels (
            tg_id,
            access_hash,
            title,
            username,
            subscribers_count
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(tg_id) DO UPDATE SET
            access_hash = COALESCE(excluded.access_hash, channels.access_hash),
            title = excluded.title,
            username = excluded.username,
            subscribers_count = excluded.subscribers_count
        """,
        (
            ch.id,
            ch.access_hash,
            ch.title,
            ch.username,
            ch.participants_count or 0,
        )
    )


def save_recommendation(conn, seed_tg_id, rec_tg_id):
    conn.execute(
        """
        INSERT OR IGNORE INTO channel_recs (
            seed_tg_id,
            recommended_tg_id
        )
        VALUES (?, ?)
        """,
        (seed_tg_id, rec_tg_id)
    )


def mark_channel_processed(conn, tg_id):
    conn.execute(
        "UPDATE channels SET recs_rcvd = 1 WHERE tg_id = ?",
        (tg_id,)
    )


# ---------------- MAIN ----------------

def main():
    try:
        with client:
            while True:
                with db_connect() as conn:

                    tg_id, access_hash = get_channel(conn)

                    if tg_id:

                        time.sleep(BASE + random.randint(0, JITTER_MAX))

                        entity = InputPeerChannel(
                            channel_id=tg_id,
                            access_hash=access_hash
                        )

                        try:
                            result = client(
                                GetChannelRecommendationsRequest(channel=entity)
                            )

                        except FloodWaitError as e:
                            logging.error(
                                f"FloodWait: channel={tg_id}, wait={e.seconds}s"
                            )
                            raise

                        except Exception as e:
                            logging.error(
                                f"Unexpected error: channel={tg_id}, {str(e)}"
                            )
                            result = None

                        if not result or not result.chats:
                            logging.warning(
                                f"Пустые рекомендации | tg_id={tg_id}"
                            )
                            mark_channel_processed(conn, tg_id)
                            continue

                        # ---------- одна транзакция ----------
                        for ch in result.chats:
                            save_channel(conn, ch)
                            save_recommendation(conn, tg_id, ch.id)

                        mark_channel_processed(conn, tg_id)

                time.sleep(INTERVAL)

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    main()
