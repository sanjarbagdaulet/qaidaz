import sqlite3
import json
import time
import re

from lingua import Language, LanguageDetectorBuilder

INTERVAL = 2
DB_PATH = "telegram_channels.db"

# Инициализация детектора Lingua
detector = LanguageDetectorBuilder.from_languages(Language.RUSSIAN, Language.KAZAKH).build()
RUS = Language.RUSSIAN
KAZ = Language.KAZAKH


def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_text(conn):

    query = """
        SELECT channel_id, id, text
        FROM messages
        WHERE kazakh_ratio IS NULL
        ORDER BY date DESC
        LIMIT 10
        """
    row = conn.execute(query).fetchone()
    if row:
        return row["channel_id"], row[", id"], row["text"]
    return None, None, None


def tokenize(text):
    text = re.sub(r"[^а-яА-ЯёЁәғқңөұүһіӘҒҚҢӨҰҮҺІ\s]", "", text)
    return text.lower().split()


def language_percentages(text):
    words = tokenize(text)
    if not words:
        return {"kazakh": 0.0, "russian": 0.0, "other": 0.0}

    kaz_count = sum(1 for w in words if detector.detect_language_of(w) == KAZ)
    rus_count = sum(1 for w in words if detector.detect_language_of(w) == RUS)
    other_count = len(words) - kaz_count - rus_count
    total = len(words)

    return {
        "kazakh": (kaz_count / total) * 100,
        "russian": (rus_count / total) * 100,
        "other": (other_count / total) * 100
    }


def main():
    while True:

        with db_connect() as conn:

            channel_id, id, text = get_text(conn)

            if channel_id:

                language_percentages(text)



        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
