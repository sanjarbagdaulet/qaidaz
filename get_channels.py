import time
import random
import logging
import mariadb

from telethon.sync import TelegramClient
from telethon.tl.functions.channels import GetChannelRecommendationsRequest
from telethon.errors import FloodWaitError
from accounts import ACC_MAIN, DB_MAIN

WORKER = "get_channels"
BASE = 300
JITTER_MAX = 300
INTERVAL = 2
MIN_SUBSCRIBERS = 1_000

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s [%(levelname)s] %(message)s',
    filename=f'{WORKER}.log'
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


def get_seed_channel(conn):
    """
            SELECT channel_id, username
            FROM channels
            WHERE c_rcvd = 0
              AND (not_kz IS NULL OR not_kz <> 1)
              AND participants_count >= %s
              AND (
                    (kk_ratio_by_l IS NOT NULL AND kk_ratio_by_l > 5)
                 OR (kk_ratio_by_f IS NOT NULL AND kk_ratio_by_f > 5)
              )
            ORDER BY participants_count DESC
            LIMIT 1
        """
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT channel_id, username
        FROM channels
        WHERE c_rcvd = 0
          AND not_kz = 2
          AND participants_count >= %s
        ORDER BY participants_count DESC
        LIMIT 1
    """, (MIN_SUBSCRIBERS,))
    row = cur.fetchone()
    cur.close()

    if row:
        return row["channel_id"], row["username"]
    return None, None


def save_channels(conn, channels):
    """
    Upsert по channel_id:
    - если новый: repeats_count = 1, not_kz=0, kk_ratio_* = 0.0, c_rcvd=0, msgs_rcvd НЕ трогаем (по умолчанию 0)
    - если уже есть: обновляем participants_count, linked_monoforum_id, repeats_count += 1
    """
    cur = conn.cursor()
    now_ts = int(time.time())

    for ch in channels:
        username = getattr(ch, "username", None)
        if not username:
            continue

        ch_date = int(ch.date.timestamp()) if getattr(ch, "date", None) else now_ts
        participants = int(ch.participants_count or 0)

        # реальный linked_monoforum_id, если есть
        linked_id = getattr(ch, "linked_monoforum_id", None)
        if linked_id is not None:
            linked_id = int(linked_id)

        try:
            cur.execute("""
                INSERT INTO channels (
                    channel_id, title, date, access_hash, username,
                    participants_count, linked_monoforum_id,
                    repeats_count, not_kz, kk_ratio_by_l, kk_ratio_by_f, c_rcvd, msgs_rcvd
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 1, 0, 0.0, 0.0, 0, 0)
                ON DUPLICATE KEY UPDATE
                    participants_count = VALUES(participants_count),
                    linked_monoforum_id = VALUES(linked_monoforum_id),
                    repeats_count = repeats_count + 1
            """, (
                int(ch.id),
                ch.title,
                ch_date,
                int(ch.access_hash),
                username,
                participants,
                linked_id
            ))
        except Exception as e:
            logging.error(f"Ошибка при сохранении канала {getattr(ch, 'id', 'unknown')}: {e}")

    cur.close()
    conn.commit()


def save_recs(conn, seed_id, channels):
    cur = conn.cursor()
    created_at = int(time.time())

    for ch in channels:
        try:
            cur.execute("""
                INSERT IGNORE INTO recs (seed_channel_id, recommended_channel_id, created_at)
                VALUES (%s, %s, %s)
            """, (int(seed_id), int(ch.id), created_at))
        except Exception as e:
            logging.error(f"Ошибка при сохранении рекомендации {seed_id} -> {getattr(ch, 'id', 'unknown')}: {e}")

    cur.close()
    conn.commit()


def mark_channel_processed(conn, channel_id):
    cur = conn.cursor()
    cur.execute("UPDATE channels SET c_rcvd = 1 WHERE channel_id = %s", (int(channel_id),))
    cur.close()
    conn.commit()


def main():
    client = TelegramClient(ACC_MAIN.session + WORKER, ACC_MAIN.api_id, ACC_MAIN.api_hash)
    client.start()

    while True:
        conn = None
        try:
            conn = db_connect()

            seed, username = get_seed_channel(conn)
            if not seed or not username:
                conn.close()
                time.sleep(INTERVAL)
                continue

            time.sleep(BASE + random.randint(0, JITTER_MAX))

            try:
                result = client(GetChannelRecommendationsRequest(channel=username))
            except FloodWaitError as e:
                logging.error(f"FloodWait: {e.seconds} секунд. Скрипт остановлен.")
                return
            except Exception as e:
                logging.error(f"Ошибка запроса рекомендаций: {e}")
                time.sleep(INTERVAL)
                continue

            save_channels(conn, result.chats)
            save_recs(conn, seed, result.chats)
            mark_channel_processed(conn, seed)

        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        time.sleep(INTERVAL)


if __name__ == '__main__':
    main()
