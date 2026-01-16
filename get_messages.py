from telethon.sync import TelegramClient
from accounts import ACC_FARM_1, DB_MAIN
from telethon.errors import FloodWaitError

import time
import random
import logging
import mariadb
import os

WORKER = "get_messages"
INTERVAL = 2
MIN_SUBSCRIBERS = 30_000
BASE = 300
JITTER_MAX = 300

a = ACC_FARM_1
client = TelegramClient(a.session + WORKER, a.api_id, a.api_hash)

logging.basicConfig(
    format='[%(levelname)s %(asctime)s] %(name)s: %(message)s',
    level=logging.ERROR,
    filename=WORKER + '.log',
)


def db_connect():
    return mariadb.connect(
        host=DB_MAIN.host,
        port=DB_MAIN.port,
        user=DB_MAIN.user,
        password=DB_MAIN.password,
        database=DB_MAIN.name,
        autocommit=False,
    )


def get_channel(conn):
    query = """
        SELECT channel_id, username
        FROM channels
        WHERE participants_count >= %s
          AND msgs_rcvd = 0
          AND (not_kz IS NULL OR not_kz <> 1)
        ORDER BY participants_count DESC
        LIMIT 1
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(query, (MIN_SUBSCRIBERS,))
    row = cur.fetchone()
    cur.close()

    if row:
        return row["channel_id"], row["username"]
    return None, None


def should_skip_message(msg):
    return msg.fwd_from is not None


def extract_media_type(msg):
    if msg.media is None:
        return "text"
    return type(msg.media).__name__


def save_message(conn, channel_id, msg):
    """
    В MariaDB эквивалент "ON CONFLICT DO NOTHING" для PK:
    INSERT IGNORE (игнорирует дубликаты по PRIMARY/UNIQUE).
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT IGNORE INTO messages (
                id,
                channel_id,
                date,
                message,
                media_type,
                kazakh_ratio
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                int(msg.id),
                int(channel_id),
                int(msg.date.timestamp()),
                msg.raw_text,
                extract_media_type(msg),
                0,
            ),
        )
        cur.close()
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")


def mark_channel_processed(conn, channel_id):
    cur = conn.cursor()
    cur.execute(
        "UPDATE channels SET msgs_rcvd = 1 WHERE channel_id = %s",
        (int(channel_id),),
    )
    cur.close()


def main():
    try:
        with client:
            while True:
                conn = None
                try:
                    conn = db_connect()

                    channel_id, username = get_channel(conn)

                    if channel_id and username:
                        time.sleep(BASE + random.randint(0, JITTER_MAX))

                        try:
                            messages = client.get_messages(username, limit=100)
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
                        conn.commit()

                    else:
                        # ничего не взяли — просто отпустим транзакцию
                        conn.rollback()

                except Exception:
                    if conn:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                    raise
                finally:
                    if conn:
                        try:
                            conn.close()
                        except Exception:
                            pass

                time.sleep(INTERVAL)

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    main()
