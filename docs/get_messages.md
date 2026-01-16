# ТЗ / Документация к скрипту `get_messages.py`

## Назначение

Скрипт `get_messages.py` предназначен для получения сообщений из каналов Telegram с помощью библиотеки **Telethon**.  
**Основная цель** — сбор данных для последующего анализа.

---

## Источники данных

- Каналы Telegram, доступные через авторизованный клиент.  
- Пересланные сообщения (`fwd_from`) **не сохраняются**.  

---

## Структура объекта Message

Используются следующие поля объекта `Message`:

| Поле       | Тип              | Описание                              | Используется в MVP |
| ---------- | ---------------- | ------------------------------------- | ------------------ |
| `id`       | int              | Уникальный идентификатор сообщения    | Да                 |
| `peer_id`  | Peer             | Канал, где опубликовано сообщение     | Да                 |
| `date`     | date             | Дата публикации                       | Да                 |
| `message`  | string           | Текст сообщения                       | Да                 |
| `media`    | MessageMedia     | Медиаконтент                          | Да                 |

> Остальные поля объекта `Message` в MVP не используются, но могут быть добавлены при необходимости.

---

## Хранение данных

- **База данных**: `telegram_channels.db`  
- **Таблица**: `messages`  
- **Структура таблицы** соответствует используемым полям Message.

### SQL-схема таблицы `messages`

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
```
### Особенности сохранения

* **Уникальность сообщений** достигается комбинацией `(id, channel_id)`.
* **Пересланные сообщения** (`fwd_from`) **не сохраняются**.
* **Сообщения без медиа** сохраняются как `media_type = "text"`.
* **Сообщения с медиа** сохраняются с `media_type = type(msg.media).__name__`.

---

### Логика работы скрипта

1. Получение канала из таблицы `channels` с минимальным количеством подписчиков и `msgs_rcvd = 0`.
2. Формирование объекта `InputPeerChannel` с `channel_id` и `access_hash`.
3. Получение последних 100 сообщений методом `client.get_messages`.
4. Фильтрация сообщений через функцию `should_skip_message`:

   * пропуск пересланных сообщений (`fwd_from is not None`)
5. Определение типа медиа через `extract_media_type(msg)`:

   * `"text"` → если `msg.media is None`
   * `type(msg.media).__name__` → если медиа присутствует
   * `"unknown"` → на всякий случай
6. Сохранение сообщений в таблицу `messages` с полями:

   * `id`, `channel_id`, `date`, `message`, `media_type`, `kazakh_ratio = 0`
7. Пометка канала как обработанного (`msgs_rcvd = 1`).
8. Скрипт работает в цикле с задержкой `BASE + random.randint(0, JITTER_MAX)` между запросами к каналу и `INTERVAL` между итерациями.
