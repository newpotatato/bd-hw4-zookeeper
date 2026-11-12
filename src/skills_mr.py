# -*- coding: utf-8 -*-
"""MapReduce из ДЗ 3 (режим stream): map-задача по одному сплиту файла в S3 и свёртка результатов.

Функции iter_split_lines и map_task_stream перенесены из hw3_mapreduce/src/mapreduce.py без изменений
логики; добавлены только сериализация выхода маппера (для чекпоинта) и сборка CSV в формате ДЗ 3.
"""
import csv
import io
import re
import time
import zlib
from collections import Counter
from itertools import combinations

from common import BUCKET, RAW_KEY, VARIANT_ROLE, is_variant, normalize_skills, s3_client

LOOKAHEAD = 1 << 20          # сколько байт дочитывать за границей сплита, чтобы закончить строку
MAX_ATTEMPTS = 3
SIZES = (1, 2, 3)            # размер «стека»: навык, пара, тройка
HEADER = ["source", "vacancy_id", "status", "snapshot_date", "published_at", "area", "employer_id",
          "employer_inn", "employer_ogrn", "employer_name", "name", "role", "sphere", "salary_from",
          "salary_to", "currency", "gross", "experience", "schedule", "employment", "key_skills",
          "description", "url"]
I_NAME, I_ROLE, I_SKILLS = HEADER.index("name"), HEADER.index("role"), HEADER.index("key_skills")


# ------------------------------------------------------------------ чтение сплита из S3
def _get(s3, a, b):
    """Байты [a, b] включительно (HTTP Range)."""
    return s3.get_object(Bucket=BUCKET, Key=RAW_KEY, Range=f"bytes={a}-{b}")["Body"].read()


def read_split(s3, start, end, size):
    """Строки, начинающиеся в диапазоне [start, end), целиком."""
    lo = max(0, start - 1)                   # байт перед сплитом: нужен, чтобы понять, начало ли это строки
    hi = min(size, end + LOOKAHEAD)
    buf = _get(s3, lo, hi - 1)
    # конец: первый '\n' на позиции >= end-1 (заканчивает строку, начавшуюся до end)
    while True:
        nl = buf.find(b"\n", end - 1 - lo)
        if nl != -1 or hi >= size:
            break
        new_hi = min(size, hi + LOOKAHEAD)
        buf += _get(s3, hi, new_hi - 1)
        hi = new_hi
    stop = nl + 1 if nl != -1 else len(buf)
    # начало: у сплита 0 - начало файла, иначе первая строка после первого '\n' в позиции >= start-1
    if start == 0:
        begin = 0
    else:
        first = buf.find(b"\n", 0)
        begin = first + 1 if first != -1 else len(buf)
    if begin >= stop:
        return b"", len(buf)
    return buf[begin:stop], len(buf)


def iter_split_lines(s3, start, end, size):
    """Потоковое чтение сплита (режим stream): строки, начинающиеся в [start, end), без загрузки сплита целиком."""
    pos = lo = max(0, start - 1)          # pos - абсолютное смещение начала текущего куска строки
    skip_first = start > 0                # до первого '\n' - хвост строки предыдущего сплита
    tail = b""
    hi = min(size, end + LOOKAHEAD)
    while lo < hi:
        body = s3.get_object(Bucket=BUCKET, Key=RAW_KEY, Range=f"bytes={lo}-{hi - 1}")["Body"]
        for chunk in body.iter_chunks(1 << 20):
            parts = (tail + chunk).split(b"\n")
            tail = parts.pop()
            for line in parts:
                line_start, pos = pos, pos + len(line) + 1
                if skip_first:
                    skip_first = False
                    continue
                if line_start >= end:     # строка начинается в следующем сплите - конец
                    body.close()
                    return
                yield line
        lo, hi = hi, min(size, hi + LOOKAHEAD)   # строка не закончилась до LOOKAHEAD - дочитываем
    if tail and not skip_first and pos < end:
        yield tail


# предфильтр по сырым байтам (режим stream): должен быть строго ШИРЕ точного условия is_variant
ROLE_B = VARIANT_ROLE.encode("utf-8")
TITLE_B = re.compile(rb"(c#|\xd1\x81#|\xd0\xa1#|\.net|dotnet)", re.IGNORECASE)   # \xd1\x81 = «с», \xd0\xa1 = «С»


def map_task_stream(task_id, start, end, size, fail_task, attempt, reducers):
    if task_id == fail_task and attempt == 1:
        raise RuntimeError(f"имитация сбоя маппера {task_id} (попытка {attempt})")
    t0 = time.perf_counter()
    s3 = s3_client()
    stats = Counter()
    combined = {k: Counter() for k in SIZES}
    for line in iter_split_lines(s3, start, end, size):
        stats["lines"] += 1
        if ROLE_B not in line or not TITLE_B.search(line):
            continue
        row = next(csv.reader([line.decode("utf-8")]))
        if len(row) != len(HEADER) or not is_variant(row[I_ROLE], row[I_NAME]):
            continue
        stats["vacancies"] += 1
        skills = normalize_skills(row[I_SKILLS])
        if not skills:
            continue
        stats["with_skills"] += 1
        for k in SIZES:
            combined[k].update(combinations(skills, k))
    parts = [{k: {} for k in SIZES} for _ in range(reducers)]
    for k, cnt in combined.items():
        for key, v in cnt.items():
            parts[zlib.crc32("\x1f".join(key).encode()) % reducers][k][key] = v
    stats["emitted"] = sum(len(c) for c in combined.values())
    stats["bytes_fetched"] = 0
    stats["map_sec"] = round(time.perf_counter() - t0, 3)
    return task_id, parts, dict(stats)


# ------------------------------------------------------------------ дополнения ДЗ 4
SEP = "\x1f"
NAMES = {1: "skills_k1.csv", 2: "pairs_k2.csv", 3: "triples_k3.csv"}


def make_splits(size, chunk_mb=64):
    chunk = chunk_mb * 2**20
    return [(i, a, min(size, a + chunk)) for i, a in enumerate(range(0, size, chunk))]


def parts_to_json(parts):
    """Выход маппера {k: {(навык, ...): n}} по партициям -> JSON-совместимый вид (ключ-кортеж склеен через \x1f)."""
    return [{str(k): {SEP.join(key): v for key, v in p[k].items()} for k in SIZES} for p in parts]


def parts_from_json(obj):
    return [{k: {tuple(key.split(SEP)): v for key, v in p[str(k)].items()} for k in SIZES} for p in obj]


def reduce_all(all_parts):
    """Свёртка: сумма частот по всем партициям всех мапперов."""
    totals = {k: Counter() for k in SIZES}
    for parts in all_parts:
        for p in parts:
            for k in SIZES:
                totals[k].update(p[k])
    return totals


def to_csv(totals, k):
    """CSV ровно в формате ДЗ 3: сортировка по убыванию частоты, затем по ключу."""
    rows = sorted(totals[k].items(), key=lambda kv: (-kv[1], kv[0]))
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow([f"skill_{j + 1}" for j in range(k)] + ["count"])
    w.writerows(list(key) + [v] for key, v in rows)
    return buf.getvalue().encode("utf-8"), rows
