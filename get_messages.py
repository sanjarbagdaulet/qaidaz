from telethon import TelegramClient
from accounts import ACC_FARM_2
from telethon.errors import FloodWaitError
from telethon.tl.types import InputPeerChannel

import time
import sqlite3
import random
import logging

WORKER = "get_messages"
INTERVAL = 2
DB_PATH = "telegram_channels.db"
MIN_SUBSCRIBERS = 100_000
BASE = 300
JITTER_MAX = 300

a = ACC_FARM_2
client = TelegramClient(a.session + WORKER, a.api_id, a.api_hash)

logging.basicConfig(
    format='[%(levelname)s %(asctime)s] %(name)s: %(message)s',
    level=logging.ERROR,
    filename=WORKER + '.log',
)


def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


def get_channel(conn):
    query = """
        SELECT channel_id, access_hash
        FROM channels
        WHERE subscribers_count >= ?
          AND msgs_rcvd = 0
        ORDER BY subscribers_count DESC
        LIMIT 1
        """
    row = conn.execute(query, (MIN_SUBSCRIBERS,)).fetchone()
    if row:
        return row["channel_id"], row["access_hash"]
    return None, None


def should_skip_message(msg):
    return msg.fwd_from is not None


def extract_media_type(msg):
    if msg.media is None:
        return "text"
    elif msg.media is not None:
        return type(msg.media).__name__
    else:
        return "unknown"


def save_message(conn, channel_id, msg):
    try:
        conn.execute(
            """
            INSERT INTO messages (
                id,
                channel_id,
                date,
                message,
                media_type,
                kazakh_ratio
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id, channel_id) DO NOTHING;
            """,
            (
                int(msg.id),  # ID сообщения
                int(channel_id),  # tg_id канала
                int(msg.date.timestamp()),  # дата публикации
                msg.raw_text,  # текст или None
                extract_media_type(msg),
                0  # kazakh_ratio пока не рассчитываем
            ),
        )
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")


def mark_channel_processed(conn, channel_id):
    conn.execute(
        "UPDATE channels SET msgs_rcvd = 1 WHERE channel_id = ?",
        (channel_id,)
    )


def main():
    try:
        with client:
            while True:
                with db_connect() as conn:

                    channel_id, access_hash = get_channel(conn)

                    if channel_id:

                        time.sleep(BASE + random.randint(0, JITTER_MAX))

                        entity = InputPeerChannel(
                            channel_id=channel_id,
                            access_hash=access_hash
                        )

                        try:
                            messages = client.get_messages(entity, limit=100)

                        except FloodWaitError as e:
                            logging.error(f"FloodWait: channel={channel_id}, wait={e.seconds}s")
                            raise
                        except Exception as e:
                            logging.error(f"Unexpected error: channel={channel_id}, {str(e)}")
                            messages = []

                        for msg in messages:
                            if should_skip_message(msg):
                                continue
                            save_message(conn, channel_id, msg)
                        mark_channel_processed(conn, channel_id)

                time.sleep(INTERVAL)

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    main()
