import sqlite3
import logging
from telethon import TelegramClient
from accounts import ACC_MAIN
from sleep_policy import sleep_before_tg_call, local_sleep, init_lock_db, WORKER_ID

# ======================================================================
# CONFIG
# ======================================================================
DB_PATH = "telegram_channels.db"
MESSAGES_LIMIT = 100
MIN_SUBSCRIBERS = 100000
CYCLE_SLEEP = 5  # пауза между итерациями цикла
LOG_FILE = "get_messages_errors.log"

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
    """
    Возвращает подключение к SQLite с возможностью обращаться к колонкам по имени.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ======================================================================
# CHANNEL SELECTION
# ======================================================================
def get_channel_to_process(conn):
    query = """
    SELECT tg_id, username
    FROM channels
    WHERE subscribers_count >= ?
      AND processed = 0
    ORDER BY subscribers_count DESC
    LIMIT 1
    """
    row = conn.execute(query, (MIN_SUBSCRIBERS,)).fetchone()
    if row:
        return row["tg_id"], row["username"]
    return None, None


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
def save_message(conn, channel_tg_id, msg):
    try:
        conn.execute(
            """
            INSERT INTO messages (
                channel_id,
                id,
                date,
                text,
                media_type,
                kazakh_ratio
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(channel_id, id) DO NOTHING;
            """,
            (
                channel_tg_id,        # tg_id канала
                msg.id,               # ID сообщения
                msg.date,             # дата публикации
                msg.message,          # текст или None
                extract_media_type(msg),
                None                  # kazakh_ratio пока не рассчитываем
            ),
        )
    except Exception:
        logger.exception(
            f"Failed to save message {msg.id} from channel {channel_tg_id}"
        )


def mark_channel_processed(conn, channel_id):
    try:
        conn.execute(
            "UPDATE channels SET processed = 1 WHERE tg_id = ?",
            (channel_id,)
        )
    except Exception as e:
        logger.exception(f"Failed to mark channel {channel_id} as processed")


# ======================================================================
# MAIN LOOP
# ======================================================================
def main():
    init_lock_db()  # создаём таблицу для координации воркеров

    try:
        with client:
            while True:
                with get_db_connection() as conn:

                    channel_id, username = get_channel_to_process(conn)
                    if not channel_id:
                        local_sleep(CYCLE_SLEEP)
                        continue

                    # ---- глобальный sleep через lock DB ----
                    entity = client.get_input_entity(username)
                    sleep_before_tg_call()

                    try:
                        messages = client.get_messages(entity, limit=MESSAGES_LIMIT)
                    except Exception as e:
                        logger.exception(f"Failed to get messages for channel {channel_id}")
                        messages = []

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
