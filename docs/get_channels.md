# ТЗ к скрипту get_channels.py

## Назначение
Скрипт `get_channels.py` предназначен для сбора списка Telegram-каналов из раздела «Похожие каналы» (Channel Recommendations) с использованием Telegram API через библиотеку Telethon.  
Основная цель — расширение базы каналов за счёт рекомендаций, полученных от уже известных каналов, подходящих по заданным критериям.

## Источник данных
Источник данных — Telegram API.  
Используется метод `GetChannelRecommendationsRequest`, который возвращает объект `ChatSlice`, содержащий:
- `count` — общее количество рекомендованных каналов
- `chats` — список объектов `Channel`

Запрос рекомендаций выполняется для конкретного канала, переданного в параметре `peer`.

## Выбор канала для запроса рекомендаций
Скрипт выбирает один канал из базы данных `telegram_channels.db` (таблица `channels`), удовлетворяющий условиям:
- `c_rcvd = 0` — рекомендации для этого канала ещё не получались
- `not_kz != 1`
- `participants_count >= MIN_SUBSCRIBERS`
- `kk_ratio_by_l > 0` или `kk_ratio_by_f > 0`

Сортировка:
- по `participants_count` по убыванию

Выбирается один канал (LIMIT 1).  
Если подходящего канала нет — итерация пропускается.

## Параметры фильтрации
- `MIN_SUBSCRIBERS = 100_000`
- Паузы между запросами API: `BASE + random.randint(0, JITTER_MAX)` секунд
- Пауза между итерациями основного цикла: `INTERVAL` секунд  

Значения этих констант задаются в коде.

## Построение entity для запроса
Для выполнения `GetChannelRecommendationsRequest`:
- берётся `channel_id` и `access_hash` из базы
- создаётся объект `InputChannel(channel_id, access_hash)`
- объект передаётся в параметр `peer` запроса

## Структура объекта Channel
Из объекта `Channel` используются следующие поля:
- `id` — уникальный идентификатор канала
- `title` — название канала
- `date` — дата создания
- `access_hash` — хэш для доступа
- `username` — юзернейм канала
- `participants_count` — количество участников
- `linked_monoforum_id` — преобразуется в флаг: `1` если есть, `0` если нет

### Правила обработки username
- Если поле `username` заполнено — используется оно
- Если `username` пустой — канал не сохраняется

## Хранение данных
База данных: `telegram_channels.db`  
Таблица: `channels`

Скрипт сохраняет новые каналы с полями:
- `channel_id`
- `title`
- `date`
- `access_hash`
- `username`
- `participants_count`
- `linked_monoforum_id` (0/1)
- `repeats_count` — увеличивается на 1 при повторной вставке
- `not_kz` — 0 по умолчанию
- `kk_ratio_by_l` — 0.0 по умолчанию
- `kk_ratio_by_f` — 0.0 по умолчанию
- `c_rcvd` — 0 для новых каналов

### Уникальность каналов
- Уникальность обеспечивается по `channel_id`
- Если канал уже есть — выполняется **Upsert**:
  - обновляется `participants_count`
  - обновляется `linked_monoforum_id`
  - увеличивается `repeats_count` на 1

### Таблица recs
- Для каждой рекомендации seed → recommended сохраняется запись в таблицу `recs` с полем `created_at` (Unix timestamp)
- Уникальность по `(seed_channel_id, recommended_channel_id)`
- При повторной вставке `DO NOTHING`

## Логика работы скрипта
1. Подключение к Telegram через аккаунт `ACC_MAIN`
2. Подключение к базе данных `telegram_channels.db`
3. Выбор одного канала из таблицы `channels` по условиям фильтрации
4. Пауза `BASE + JITTER_MAX` перед запросом API
5. Выполнение запроса `GetChannelRecommendationsRequest(peer=InputChannel)`
6. Обработка списка рекомендованных каналов:
   - сохранение новых каналов в таблицу `channels` через Upsert
   - сохранение рекомендаций в таблицу `recs`
7. Пометка исходного канала как обработанного (`c_rcvd = 1`)
8. Пауза между итерациями цикла: `INTERVAL` секунд

## Ограничения и допущения
- Скрипт не обрабатывает приватные каналы без `access_hash`
- FloodWait полностью останавливает скрипт и логируется
- Скрипт рассчитан на бесконечный цикл с возможностью постоянного запуска
- Поля `repeats_count`, `not_kz`, `kk_ratio_by_l`, `kk_ratio_by_f` могут обновляться другими скриптами

## Создание таблиц
```sql
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

CREATE TABLE IF NOT EXISTS recs (
    seed_channel_id INTEGER NOT NULL,
    recommended_channel_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (seed_channel_id, recommended_channel_id)
);
