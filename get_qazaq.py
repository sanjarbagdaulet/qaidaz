import sqlite3
import time
import logging
from lingua import LanguageDetectorBuilder, Language

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# Инициализация детектора Lingua (казахский + русский + английский)
detector = LanguageDetectorBuilder.from_languages(Language.KAZAKH, Language.RUSSIAN, Language.ENGLISH)\
                                  .build()

DB_PATH = 'telegram_channels.db'  # путь к SQLite

SLEEP_INTERVAL = 4  # сек


def calculate_kazakh_ratio(text: str) -> int:
    """
    Возвращает процент слов на казахском в тексте.
    """
    if not text:
        return 0
    words = [w.strip() for w in text.split() if w.strip()]
    if not words:
        return 0
    kazakh_count = sum(1 for w in words if detector.detect_language_of(w) == Language.KAZAKH)
    return int((kazakh_count / len(words)) * 100)


def process_channel_messages(conn, channel_id):
    """
    Обрабатывает все сообщения канала, у которых kazakh_ratio=0
    """
    cursor = conn.cursor()

    while True:
        cursor.execute("""
            SELECT id, message
            FROM messages
            WHERE channel_id = ? AND kazakh_ratio = 0
            ORDER BY date ASC
            LIMIT 1
        """, (channel_id,))
        row = cursor.fetchone()
        if not row:
            break  # все сообщения канала обработаны

        msg_id, msg_text = row
        try:
            ratio = calculate_kazakh_ratio(msg_text)
        except Exception as e:
            logging.error(f"Ошибка анализа сообщения {msg_id}: {e}")
            ratio = 0

        cursor.execute("""
            UPDATE messages
            SET kazakh_ratio = ?
            WHERE id = ? AND channel_id = ?
        """, (ratio, msg_id, channel_id))
        conn.commit()
        logging.info(f"Processed message {msg_id} of channel {channel_id}, kazakh_ratio={ratio}")

        time.sleep(0.5)  # небольшой sleep, чтобы не перегружать CPU


def update_channel_ratio(conn, channel_id):
    """
    Вычисляет kk_ratio_by_l для канала и обновляет таблицу channels
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT AVG(kazakh_ratio)
        FROM messages
        WHERE channel_id = ?
    """, (channel_id,))
    avg_ratio = cursor.fetchone()[0] or 0

    cursor.execute("""
        UPDATE channels
        SET kk_ratio_by_l = ?
        WHERE channel_id = ?
    """, (avg_ratio, channel_id))
    conn.commit()
    logging.info(f"Updated kk_ratio_by_l for channel {channel_id}: {avg_ratio:.2f}")


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    logging.info("Starting Kazakh ratio analyzer...")

    while True:
        cursor = conn.cursor()
        # Выбираем канал, у которого есть сообщения с kazakh_ratio=0
        cursor.execute("""
            SELECT DISTINCT channel_id
            FROM messages
            WHERE kazakh_ratio = 0
            ORDER BY channel_id ASC
            LIMIT 1
        """)
        row = cursor.fetchone()
        if row:
            channel_id = row['channel_id']
            logging.info(f"Start processing channel {channel_id}")
            process_channel_messages(conn, channel_id)
            update_channel_ratio(conn, channel_id)
        else:
            logging.info("No new messages to process. Sleeping...")
            time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    main()
