import sqlite3
import logging
from telethon import TelegramClient
from accounts import ACC_MAIN
from sleep_policy import sleep_before_tg_call, local_sleep, init_lock_db

from telethon.tl.functions.channels import GetChannelRecommendationsRequest
from telethon.errors import FloodWaitError, RPCError

# ======================================================================
# CONFIG
# ======================================================================
SCRIPT_NAME = "get_channels"

DB_PATH = "telegram_channels.db"
CYCLE_SLEEP = 5  # пауза между итерациями цикла
LOG_FILE = f"worker_{SCRIPT_NAME}.log"
MIN_SUBSCRIBERS = 100_000

# используем session с суффиксом имени скрипта
a = ACC_MAIN
SESSION_FILE = f"{a.session}_{SCRIPT_NAME}"
client = TelegramClient(SESSION_FILE, a.api_id, a.api_hash)

# ======================================================================
# LOGGING
# ======================================================================
logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filename=LOG_FILE,
    filemode="a",
)
logger = logging.getLogger("worker_base")


# ======================================================================
# DB CONNECTION
# ======================================================================
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_channel_to_fetch(conn, min_subscribers):
    query = """
    SELECT tg_id, username
    FROM channels
    WHERE participants_count >= ?
      AND channels_rcvd = 0
    ORDER BY participants_count DESC
    LIMIT 1
    """
    row = conn.execute(query, (min_subscribers,)).fetchone()
    if row:
        return {"tg_id": row["tg_id"], "username": row["username"]}
    return None


def fetch_recommendations_for_channel(client, username):
    try:
        result = client(GetChannelRecommendationsRequest(channel=username))
        return result.chats
    except FloodWaitError as e:
        logger.error(f"FloodWaitError при получении рекомендаций для {username}: {e}")
        raise
    except RPCError as e:
        logger.error(f"RPCError при получении рекомендаций для {username}: {e}")
        return []


def save_channel(conn, ch):
    try:
        if getattr(ch, "username", None):
            db_username = ch.username
        elif getattr(ch, "usernames", None) and len(ch.usernames) > 0:
            db_username = ch.usernames[0].username
        else:
            db_username = None

        conn.execute(
            """
            INSERT INTO channels (
                tg_id, title, username, participants_count, has_comments_chat,
                access_hash, channels_rcvd, repeats_count, last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, 0, 0, CURRENT_TIMESTAMP)
            ON CONFLICT(tg_id) DO UPDATE SET
                title = excluded.title,
                username = excluded.username,
                participants_count = excluded.participants_count,
                has_comments_chat = excluded.has_comments_chat,
                access_hash = excluded.access_hash,
                repeats_count = repeats_count + 1,
                last_updated = CURRENT_TIMESTAMP
            """,
            (
                ch.id,
                getattr(ch, "title", None),
                db_username,
                getattr(ch, "participants_count", None),
                1 if getattr(ch, "has_comments_chat", False) else 0,
                getattr(ch, "access_hash", None),
            ),
        )
    except Exception:
        logger.exception(f"Failed to save channel {getattr(ch, 'username', ch.id)}")


def save_recommendation(conn, source_tg_id, recommended_tg_id):
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO recommendations (
                source_channel_id, recommended_channel_id, date_checked
            )
            VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
            (source_tg_id, recommended_tg_id),
        )
    except Exception:
        logger.exception(f"Failed to save recommendation {source_tg_id} -> {recommended_tg_id}")


def mark_channel_processed(conn, tg_id):
    try:
        conn.execute(
            "UPDATE channels SET channels_rcvd = 1, last_updated = CURRENT_TIMESTAMP WHERE tg_id = ?",
            (tg_id,)
        )
    except Exception:
        logger.exception(f"Failed to mark channel {tg_id} as processed")


# ======================================================================
# MAIN LOOP
# ======================================================================
def main():
    init_lock_db()  # глобальная синхронизация воркеров

    with client:
        while True:
            with get_db_connection() as conn:

                # 1. Берём канал для запроса рекомендаций
                source_channel = get_channel_to_fetch(conn, MIN_SUBSCRIBERS)
                if not source_channel:
                    local_sleep(CYCLE_SLEEP)
                    continue

                # Если нет username, просто помечаем канал как обработанный
                if not source_channel["username"]:
                    mark_channel_processed(conn, source_channel["tg_id"])
                    continue

                # 2. Глобальный sleep перед вызовом API
                sleep_before_tg_call()

                # 3. Получаем список рекомендованных каналов
                try:
                    recommended_channels = fetch_recommendations_for_channel(
                        client, source_channel["username"]
                    )
                except FloodWaitError:
                    logger.error(f"FloodWaitError при обработке канала {source_channel['username']}")
                    raise

                # 4. Сохраняем все рекомендованные каналы и рекомендации
                for ch in recommended_channels:
                    save_channel(conn, ch)
                    save_recommendation(conn, source_channel["tg_id"], ch.id)

                # 5. Отмечаем, что по исходному каналу сделали запрос
                mark_channel_processed(conn, source_channel["tg_id"])

                # 6. Commit изменений
                conn.commit()

            # 7. Локальный sleep между итерациями
            local_sleep(CYCLE_SLEEP)


if __name__ == "__main__":
    main()
