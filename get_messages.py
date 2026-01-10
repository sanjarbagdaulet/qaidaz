import time
import random
from sleep_policy import connect_db, get_next_worker, update_worker_time

MY_WORKER = "get_messages"  # просто меняем здесь имя воркера для разных процессов

conn, c = connect_db()

CHECK_INTERVAL = 2   # проверка, если очередь не моя
BASE_SLEEP = 5       # базовое ожидание после запроса (сек)
JITTER_MAX = 5       # джиттер в секундах


def do_tg_query(worker):
    pass


while True:
    next_worker = get_next_worker(c)

    if next_worker != MY_WORKER:
        time.sleep(CHECK_INTERVAL)
        continue

    do_tg_query(MY_WORKER)

    sleep_time = BASE_SLEEP + random.randint(0, JITTER_MAX)
    time.sleep(sleep_time)

    update_worker_time(c, conn, MY_WORKER)
