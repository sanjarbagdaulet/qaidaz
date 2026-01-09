import re
import sqlite3
from lingua import Language, LanguageDetectorBuilder

DB_PATH = "telegram_channels.db"
MIN_WORD_LENGTH = 2

detector = LanguageDetectorBuilder.from_languages(
    Language.KAZAKH,
    Language.RUSSIAN
).build()


def kazakh_ratio(text: str) -> float:
    if not text:
        return 0.0
    text = text.lower()
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"[^a-zа-яәіңғүұқөһ\s]", " ", text)
    words = [w for w in text.split() if len(w) >= MIN_WORD_LENGTH]
    if not words:
        return 0.0
    kk_count = sum(1 for w in words if detector.detect_language_of(w) == Language.KAZAKH)
    return round(kk_count / len(words), 3)


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Получаем все текстовые сообщения без казахо-ратио
    cur.execute("""
        SELECT channel_id, id, text
        FROM messages
        WHERE text IS NOT NULL AND kazakh_ratio IS NULL
    """)
    rows = cur.fetchall()

    channel_ratios = {}  # key: channel_id, value: [ratios]

    for row in rows:
        ratio = kazakh_ratio(row["text"])
        cur.execute("""
            UPDATE messages
            SET kazakh_ratio = ?
            WHERE channel_id = ? AND id = ?
        """, (ratio, row["channel_id"], row["id"]))
        channel_ratios.setdefault(row["channel_id"], []).append(ratio)

    # Обновляем channels.kazakh_ratio_avg
    for channel_id, ratios in channel_ratios.items():
        avg_ratio = round(sum(ratios) / len(ratios), 3)
        cur.execute("""
            UPDATE channels
            SET kazakh_ratio_avg = ?
            WHERE tg_id = ?
        """, (avg_ratio, channel_id))

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
