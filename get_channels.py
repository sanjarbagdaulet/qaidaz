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
DB_PATH = "telegram_channels.db"
CYCLE_SLEEP = 5  # пауза между итерациями цикла
LOG_FILE = "worker_base.log"
MIN_SUBSCRIBERS = 100_000

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
logger = logging.getLogger("worker_base")


# ======================================================================
# DB CONNECTION
# ======================================================================
def get_db_connection():
    """
    Возвращает подключение к SQLite с возможностью обращаться к колонкам по имени.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_channel_to_fetch(conn, min_subscribers):
    """
    Получает из базы один канал для запроса рекомендаций.
    Выбирает канал с достаточным числом подписчиков и ещё не обработанный.

    :param conn: объект подключения к SQLite
    :param min_subscribers: минимальное количество участников
    :return: словарь с tg_id и username или None
    """
    query = """
    SELECT tg_id, username
    FROM channels
    WHERE participants_count >= ?
      AND processed = 0
    ORDER BY participants_count DESC
    LIMIT 1
    """
    row = conn.execute(query, (min_subscribers,)).fetchone()
    if row:
        return {"tg_id": row["tg_id"], "username": row["username"]}
    return None


def fetch_recommendations_for_channel(client, username):
    """
    Получает список рекомендованных каналов для указанного канала по username.

    :param client: Telethon client
    :param username: str, username канала
    :return: список объектов Channel
    """
    try:
        result = client(GetChannelRecommendationsRequest(channel=username))
        return result.chats  # список Channel
    except FloodWaitError as e:
        logger.error(f"FloodWaitError при получении рекомендаций для {username}: {e}")
        raise  # Жёстко падаем
    except RPCError as e:
        logger.error(f"RPC error при получении рекомендаций для {username}: {e}")
        return []


def save_channel(conn, ch):
    """
    Сохраняет канал в таблицу channels.
    Если канал уже есть, обновляет информацию.
    :param conn: sqlite3 connection
    :param ch: объект Channel от Telethon
    """
    try:
        conn.execute(
            """
            INSERT INTO channels (
                tg_id, title, username, participants_count, has_comments_chat,
                access_hash, channels_rcvd, repeats_count, last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, 1, 0, CURRENT_TIMESTAMP)
            ON CONFLICT(tg_id) DO UPDATE SET
                title = excluded.title,
                username = excluded.username,
                participants_count = excluded.participants_count,
                has_comments_chat = excluded.has_comments_chat,
                access_hash = excluded.access_hash,
                channels_rcvd = channels_rcvd + 1,
                repeats_count = repeats_count + 1,
                last_updated = CURRENT_TIMESTAMP
            """,
            (
                ch.id,
                getattr(ch, "title", None),
                getattr(ch, "username", None),
                getattr(ch, "participants_count", None),
                1 if getattr(ch, "has_comments_chat", False) else 0,
                getattr(ch, "access_hash", None),
            ),
        )
    except Exception:
        logger.exception(f"Failed to save channel {getattr(ch, 'username', ch.id)}")


def save_recommendation(conn, source_tg_id, recommended_tg_id):
    """
    Сохраняет факт, что один канал рекомендовал другой.
    :param conn: sqlite3 connection
    :param source_tg_id: tg_id исходного канала
    :param recommended_tg_id: tg_id рекомендованного канала
    """
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
    """
    Отмечает, что по исходному каналу уже делали запрос рекомендаций.
    Для этого увеличиваем channels_rcvd.
    :param conn: sqlite3 connection
    :param tg_id: tg_id исходного канала
    """
    try:
        conn.execute(
            "UPDATE channels SET channels_rcvd = channels_rcvd + 1, last_updated = CURRENT_TIMESTAMP WHERE tg_id = ?",
            (tg_id,)
        )
    except Exception:
        logger.exception(f"Failed to mark channel {tg_id} as processed (channels_rcvd)")


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

                # 2. Глобальный sleep перед вызовом API
                sleep_before_tg_call()

                # 3. Получаем список рекомендованных каналов
                try:
                    recommended_channels = fetch_recommendations_for_channel(
                        client, source_channel["username"]
                    )
                except FloodWaitError:
                    logger.error(f"FloodWaitError при обработке канала {source_channel['username']}")
                    raise  # Жёстко падаем

                # 4. Сохраняем все рекомендованные каналы и рекомендации
                for ch in recommended_channels:
                    save_channel(conn, ch)  # сохраняем канал
                    save_recommendation(conn, source_channel["tg_id"], ch.id)  # связь источник -> рекомендация

                # 5. Отмечаем, что по исходному каналу сделали запрос
                mark_channel_processed(conn, source_channel["tg_id"])

                # 6. Commit изменений
                conn.commit()

            # 7. Локальный sleep между итерациями
            local_sleep(CYCLE_SLEEP)


if __name__ == "__main__":
    main()
