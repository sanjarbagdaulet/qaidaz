# Техническое задание (ТЗ)  
**Проект:** Серверный анализ казахоязычности Telegram-постов  
**Платформа:** Python + SQLite  
**Цель:** Определять процент казахского языка в Telegram-постах и сохранять результаты в БД.

**Ограничения VPS:** 1 CPU, 1 ГБ RAM, 10 ГБ SSD — скрипт должен экономно использовать ресурсы.  
**Библиотеки:** `lingua-language-detector`, `sqlite3`, `logging`  
**Интервал проверки новых сообщений:** 4 секунды  

---

## 1. Функциональные требования

1. Скрипт выбирает канал с непросмотренными сообщениями (`kazakh_ratio = 0`) и обрабатывает **все сообщения этого канала**.  
2. Для каждого сообщения вычисляется `kazakh_ratio` — процент слов на казахском языке с использованием Lingua.  
3. Результат сохраняется в таблице `messages` в поле `kazakh_ratio`.  
4. После обработки всех сообщений канала пересчитывается среднее значение `kk_ratio_by_l` и сохраняется в таблице `channels`.  
5. Скрипт работает **непрерывно**, проверяя наличие новых сообщений каждые 4 секунды.  
6. Логируются все ошибки анализа и прогресс обработки.  

---

## 2. Алгоритм работы скрипта

1. Подключение к БД (`sqlite3`).  
2. Бесконечный цикл:  
   1. Выбор канала с сообщениями, у которых `kazakh_ratio = 0`.  
   2. Для выбранного канала обрабатывать все его сообщения по одному:  
      - Получение текста сообщения.  
      - Вычисление `kazakh_ratio`.  
      - Сохранение результата в таблицу `messages`.  
   3. После завершения обработки канала пересчитать `kk_ratio_by_l` в таблице `channels`.  
   4. Переход к следующему каналу или ожидание 4 секунды, если новых сообщений нет.  

---

## 3. Ограничения и рекомендации

- Обрабатывать сообщения по одному для экономии ресурсов VPS.  
- Не запускать несколько экземпляров скрипта одновременно.  
- Использовать индексы в БД (`channel_id` в `messages`) для ускорения выборки.  
- Логировать ошибки, чтобы не пропустить сообщения, которые не удалось проанализировать.  
- Тайм-аут между обработкой каналов/проверками новых сообщений: 4 секунды.

---

## 4. Структура базы данных

### 4.1 Таблица `messages`

```sql
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    date INTEGER NOT NULL,           -- UNIX timestamp
    message TEXT,
    media_type TEXT NOT NULL,        -- type(msg.media).__name__ или "text"
    kazakh_ratio INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (id, channel_id)
);


CREATE TABLE IF NOT EXISTS channels (
    channel_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    date INTEGER,
    access_hash INTEGER NOT NULL,
    username TEXT,
    participants_count INTEGER NOT NULL,
    linked_monoforum_id INTEGER,
    repeats_count INTEGER,
    not_kz INTEGER,
    kk_ratio_by_l REAL,
    kk_ratio_by_f REAL,
    c_rcvd INTEGER NOT NULL DEFAULT 0
);
```