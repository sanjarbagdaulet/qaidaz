# kazakh_ratio_worker.py

import re
import time
import sqlite3
from lingua import Language, LanguageDetectorBuilder

# ================== НАСТРОЙКИ ==================
DB_PATH = "telegram_channels.db"  # путь к базе
BATCH_SIZE = 20                   # сколько сообщений обрабатываем за раз
LOOP_SLEEP = 2                     # пауза между циклами (секунды)
MIN_WORD_LENGTH = 2                # минимальная длина слова для анализа
# ===============================================

# Создаём детектор языка для казахского и русского
detector = LanguageDetectorBuilder.from_languages(
    Language.KAZAKH,
    Language.RUSSIAN
).build()


def kazakh_ratio(text: str) -> float:
    """
    Рассчитывает долю слов на казахском языке в тексте.
    Возвращает значение от 0.0 до 1.0
    """
    if not text:
        return 0.0

    # очистка текста: ссылки, спецсимволы
    text = text.lower()
    text = re.sub(r"http\S+", "", text)              # убираем ссылки
    text = re.sub(r"[^a-zа-яәіңғүұқөһ\s]", " ", text)  # оставляем только буквы

    words = [w for w in text.split() if len(w) >= MIN_WORD_LENGTH]
    if not words:
        return 0.0

    kk_count = 0
    for w in words:
        if detector.detect_language_of(w) == Language.KAZAKH:
            kk_count += 1

    return round(kk_count / len(words), 3)


def worker_loop():
    """
    Главный цикл воркера.
    Берёт партии сообщений без kazakh_ratio, анализирует и обновляет.
    Работает бесконечно.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    while True:
        # получаем batch сообщений без kazakh_ratio
        cur.execute("""
            SELECT channel_tg_id, message_id, text
            FROM messages
            WHERE is_text = 1
              AND kazakh_ratio IS NULL
            LIMIT ?
        """, (BATCH_SIZE,))

        rows = cur.fetchall()

        if not rows:
            # если ничего нет, ждем и повторяем
            time.sleep(LOOP_SLEEP)
            continue

        for channel_id, msg_id, text in rows:
            ratio = kazakh_ratio(text)
            cur.execute("""
                UPDATE messages
                SET kazakh_ratio = ?
                WHERE channel_tg_id = ? AND message_id = ?
            """, (ratio, channel_id, msg_id))

        conn.commit()
        time.sleep(LOOP_SLEEP)


if __name__ == "__main__":
    worker_loop()
