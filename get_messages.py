import sqlite3
import logging

from telethon import TelegramClient
from accounts import ACC_MAIN
from sleep_policy import sleep_before_tg_call, mark_tg_call, local_sleep

# ======================================================================
# CONFIG check
# ======================================================================

DB_PATH = "telegram_channels.db"
MESSAGES_LIMIT = 100
MIN_SUBSCRIBERS = 100000
CYCLE_SLEEP = 5  # пауза между итерациями цикла
LOG_FILE = "get_messages_errors.log"  # ошибки пишем сюда

a = ACC_MAIN
client = TelegramClient(a.session, a.api_id, a.api_hash)

# ======================================================================
# LOGGING
# ======================================================================

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filename=LOG_FILE,
    filemode="a",
)

logger = logging.getLogger("get_messages")

# ======================================================================
# DB
# ======================================================================


def get_db_connection():
    return sqlite3.connect(DB_PATH)


# ======================================================================
# CHANNEL SELECTION
# ======================================================================

def get_channel_to_process(conn):
    query = """
    SELECT id, tg_id
    FROM channels
    WHERE subscribers_count >= ?
      AND processed = 0
    ORDER BY subscribers_count DESC
    LIMIT 1
    """
    return conn.execute(query, (MIN_SUBSCRIBERS,)).fetchone()


# ======================================================================
# MESSAGE FILTERING
# ======================================================================

def should_skip_message(msg):
    if msg.fwd_from is not None:
        return True
    if msg.message is None and msg.media is None:
        return True
    return False


# ======================================================================
# MESSAGE NORMALIZATION
# ======================================================================

def extract_media_type(msg):
    return "text" if msg.media is None else type(msg.media).__name__


# ======================================================================
# PERSISTENCE
# ======================================================================

def save_message(conn, channel_id, msg):
    """
    Сохраняет сообщение в таблицу messages.

    Аргументы:
        conn        : sqlite3.Connection
        channel_id  : int  - локальный ID канала
        msg         : telethon.tl.types.Message
    """

    try:
        conn.execute(
            """
            INSERT INTO messages (
                id, channel_id, text, timestamp, media_type, is_text
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO NOTHING;
            """,
            (
                msg.id,
                channel_id,
                msg.message,
                msg.date.isoformat(),
                extract_media_type(msg),
                1 if msg.message else 0
            ),
        )
    except Exception as e:
        logger.exception(f"Failed to save message {msg.id} from channel {channel_id}")


def mark_channel_processed(conn, channel_id):
    """
    Помечает канал как обработанный (processed = 1).

    Аргументы:
        conn        : sqlite3.Connection
        channel_id  : int
    """
    try:
        conn.execute(
            "UPDATE channels SET processed = 1 WHERE id = ?",
            (channel_id,)
        )
    except Exception as e:
        logger.exception(f"Failed to mark channel {channel_id} as processed")


# ======================================================================
# MAIN LOOP
# ======================================================================

def main():
    try:
        with client:
            while True:
                with get_db_connection() as conn:

                    channel = get_channel_to_process(conn)

                    if not channel:
                        conn.close()
                        local_sleep(CYCLE_SLEEP)
                        continue

                    channel_id, tg_id = channel

                    # глобальный sleep перед TG API
                    sleep_before_tg_call()
                    try:
                        messages = client.get_messages(tg_id, limit=MESSAGES_LIMIT)
                    except Exception as e:
                        logger.exception(f"Failed to get messages for channel {tg_id}")
                        messages = []  # пустой список, чтобы цикл ниже ничего не сохранял
                    finally:
                        mark_tg_call()

                    for msg in messages:
                        if should_skip_message(msg):
                            continue
                        save_message(conn, channel_id, msg)

                    mark_channel_processed(conn, channel_id)
                    conn.commit()

                local_sleep(CYCLE_SLEEP)

    except Exception as e:
        logger.exception("Unhandled error in get_messages worker")


if __name__ == "__main__":
    main()
