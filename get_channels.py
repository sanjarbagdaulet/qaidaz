import sqlite3
import time
import random
import logging

from telethon.sync import TelegramClient
from telethon.tl.functions.channels import GetChannelRecommendationsRequest
from telethon.tl.types import InputChannel
from telethon.errors import FloodWaitError
from accounts import ACC_MAIN

WORKER = "get_channels"
BASE = 300
JITTER_MAX = 300
INTERVAL = 2
MIN_SUBSCRIBERS = 100_000
DB_PATH = 'telegram_channels.db'

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s [%(levelname)s] %(message)s',
    filename=f'{WORKER}.log'
)


def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


def get_seed_channel(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT channel_id, access_hash 
        FROM channels
        WHERE c_rcvd = 0
          AND not_kz != 1
          AND participants_count >= ?
          AND (kk_ratio_by_l > 0 OR kk_ratio_by_f > 0)
        ORDER BY participants_count DESC
        LIMIT 1
    """, (MIN_SUBSCRIBERS,))
    row = cur.fetchone()
    if row:
        return InputChannel(channel_id=row['channel_id'], access_hash=row['access_hash'])
    return None


def save_channels(conn, channels):
    cur = conn.cursor()
    for ch in channels:
        username = ch.username
        if not username:
            continue

        linked_flag = 1 if getattr(ch, 'linked_monoforum_id', None) else 0

        try:
            # Upsert: если канал есть, увеличиваем repeats_count и обновляем participants_count и linked_flag
            cur.execute("""
                INSERT INTO channels (
                    channel_id, title, date, access_hash, username,
                    participants_count, linked_monoforum_id,
                    repeats_count, not_kz, kk_ratio_by_l, kk_ratio_by_f, c_rcvd
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 0, 0.0, 0.0, 0)
                ON CONFLICT(channel_id) DO UPDATE SET
                    participants_count=excluded.participants_count,
                    linked_monoforum_id=excluded.linked_monoforum_id,
                    repeats_count=channels.repeats_count + 1
            """, (
                ch.id,
                ch.title,
                int(ch.date.timestamp()) if ch.date else int(time.time()),
                ch.access_hash,
                username,
                ch.participants_count or 0,
                linked_flag
            ))
        except Exception as e:
            logging.error(f"Ошибка при сохранении канала {ch.id}: {e}")

    conn.commit()


def save_recs(conn, seed_id, channels):
    cur = conn.cursor()
    for ch in channels:
        try:
            cur.execute("""
                INSERT INTO recs (seed_channel_id, recommended_channel_id, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(seed_channel_id, recommended_channel_id) DO NOTHING
            """, (seed_id, ch.id, int(time.time())))
        except Exception as e:
            logging.error(f"Ошибка при сохранении рекомендации {seed_id} -> {ch.id}: {e}")
    conn.commit()


def mark_channel_processed(conn, channel_id):
    cur = conn.cursor()
    cur.execute("UPDATE channels SET c_rcvd = 1 WHERE channel_id = ?", (channel_id,))
    conn.commit()


def main():
    conn = db_connect()
    client = TelegramClient(ACC_MAIN.session + WORKER, ACC_MAIN.api_id, ACC_MAIN.api_hash)
    client.start()

    while True:
        seed = get_seed_channel(conn)
        if not seed:
            logging.error("Нет подходящего канала для обработки. Пропуск итерации.")
            time.sleep(INTERVAL)
            continue

        # Пауза перед запросом API
        time.sleep(BASE + random.randint(0, JITTER_MAX))

        try:
            result = client(GetChannelRecommendationsRequest(peer=seed))
        except FloodWaitError as e:
            logging.error(f"FloodWait: {e.seconds} секунд. Скрипт остановлен.")
            return
        except Exception as e:
            logging.error(f"Ошибка запроса рекомендаций: {e}")
            time.sleep(INTERVAL)
            continue

        save_channels(conn, result.chats)
        save_recs(conn, seed.channel_id, result.chats)
        mark_channel_processed(conn, seed.channel_id)

        time.sleep(INTERVAL)


if __name__ == '__main__':
    main()
