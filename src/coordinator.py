# -*- coding: utf-8 -*-
"""Координатор расчёта пар навыков (MapReduce из ДЗ 3) с выбором лидера через ZooKeeper.

Запускается в нескольких копиях. Считает только лидер, остальные ждут в очереди выборов.
  * выборы: рецепт Election (эфемерные последовательные узлы в /hw4/<run>/election);
  * эпоха: новый лидер увеличивает счётчик /hw4/<run>/epoch (сравнение по версии узла);
  * чекпоинт: после каждой map-задачи её выход кладётся в S3, а в ZooKeeper создаётся
    постоянный узел /hw4/<run>/checkpoint/task-NN; новый лидер пропускает готовые задачи;
  * результат: CSV пишутся в S3 в свой каталог эпохи results/hw4/<run>/epoch-N/,
    затем переключается указатель /hw4/<run>/result;
  * FENCING=1: указатель меняется транзакцией check(epoch, моя версия) + setData(result).
    Если после моих выборов эпоха выросла (выбран другой лидер), транзакция отклоняется с BadVersionError.

Переменные окружения: ZK_HOSTS, SESSION_TIMEOUT (с, по умолчанию 10), FENCING (0/1).
Журнал: logs/<run>_<name>.log (и копия в stdout).

Пример: FENCING=1 python src/coordinator.py --name A --run fencing
"""
import argparse
import hashlib
import json
import logging
import os
import pathlib
import sys
import time
from datetime import datetime

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError

import skills_mr as mr
from common import BUCKET, RAW_KEY, s3_client

ROOT = pathlib.Path(__file__).resolve().parent.parent
ZK_HOSTS = os.getenv("ZK_HOSTS", "localhost:2181,localhost:2182,localhost:2183")
SESSION_TIMEOUT = float(os.getenv("SESSION_TIMEOUT", "10"))
FENCING = os.getenv("FENCING", "0") == "1"
HW3_PREFIX = "results/mapreduce/"          # результат ДЗ 3 в бакете hh-lake - эталон для сверки
REDUCERS = 4


class Coordinator:
    def __init__(self, name, run, chunk_mb):
        self.name, self.run, self.chunk_mb = name, run, chunk_mb
        self.base = f"/hw4/{run}"
        self.p_election, self.p_epoch = f"{self.base}/election", f"{self.base}/epoch"
        self.p_ckpt, self.p_result = f"{self.base}/checkpoint", f"{self.base}/result"
        self.log = self._logger()
        self.s3 = s3_client()
        self.zk = KazooClient(hosts=ZK_HOSTS, timeout=SESSION_TIMEOUT)
        self.zk.add_listener(self._on_state)
        self.epoch = self.epoch_version = None
        self.exit_code = 0
        self.stopping = False

    # ------------------------------------------------------------------ служебное
    def _logger(self):
        (ROOT / "logs").mkdir(exist_ok=True)
        lg = logging.getLogger(f"coord-{self.name}")
        lg.setLevel(logging.INFO)
        fmt = logging.Formatter(f"%(asctime)s.%(msecs)03d [{self.name} pid={os.getpid()}] %(message)s",
                                "%Y-%m-%d %H:%M:%S")
        fh = logging.FileHandler(ROOT / "logs" / f"{self.run}_{self.name}.log", mode="w", encoding="utf-8")
        sh = logging.StreamHandler(sys.stdout)
        for h in (fh, sh):
            h.setFormatter(fmt)
            lg.addHandler(h)
        # предупреждения самой библиотеки kazoo (обрыв соединения, истечение сессии) - в тот же журнал
        kz = logging.getLogger("kazoo")
        kz.setLevel(logging.WARNING)
        kfmt = logging.Formatter(f"%(asctime)s.%(msecs)03d [{self.name} pid={os.getpid()}] kazoo: %(message)s",
                                 "%Y-%m-%d %H:%M:%S")
        for h in (logging.FileHandler(ROOT / "logs" / f"{self.run}_{self.name}.log", encoding="utf-8"),
                  logging.StreamHandler(sys.stdout)):
            h.setFormatter(kfmt)
            kz.addHandler(h)
        return lg.info

    def _session(self):
        cid = self.zk.client_id
        return f"0x{cid[0]:x}" if cid else "-"

    def _on_state(self, state):
        # вызывается из потока kazoo: только пишем в журнал
        if self.stopping:
            return
        notes = {"LOST": "сессия истекла: эфемерные узлы этого клиента удалены сервером",
                 "SUSPENDED": "связь с сервером прервана, ждём переподключения",
                 "CONNECTED": "соединение есть"}
        self.log(f"ZooKeeper: состояние {state} ({notes.get(state, '')}), сессия {self._session()}")

    def setup(self):
        self.zk.start(timeout=30)
        self.log(f"подключён к {ZK_HOSTS}; сессия {self._session()}, тайм-аут {self.zk._session_timeout / 1000:.0f} с; "
                 f"FENCING={int(FENCING)}")
        for p in (self.p_election, self.p_ckpt):
            self.zk.ensure_path(p)
        for p, init in ((self.p_epoch, b"0"), (self.p_result, b"")):
            try:
                self.zk.create(p, init)
            except NodeExistsError:
                pass

    # ------------------------------------------------------------------ лидерство
    def run_forever(self):
        self.setup()
        election = self.zk.Election(self.p_election, identifier=self.name)
        self.log(f"встал в очередь выборов {self.p_election}; сейчас в очереди: {election.contenders() or '-'}")
        election.run(self.lead)
        self.stopping = True
        self.zk.stop()
        self.log(f"сессия закрыта, работа завершена (код {self.exit_code})")
        return self.exit_code

    def bump_epoch(self):
        """Новый лидер увеличивает эпоху. setData с версией: если два лидера одновременно, второй повторит."""
        while True:
            data, st = self.zk.get(self.p_epoch)
            new = int(data or b"0") + 1
            try:
                st2 = self.zk.set(self.p_epoch, str(new).encode(), version=st.version)
                return new, st2.version
            except BadVersionError:
                continue

    def lead(self):
        self.log("СТАЛ ЛИДЕРОМ")
        self.epoch, self.epoch_version = self.bump_epoch()
        self.log(f"эпоха {self.epoch} (узел {self.p_epoch}: версия {self.epoch_version})")
        size = self.s3.head_object(Bucket=BUCKET, Key=RAW_KEY)["ContentLength"]
        splits = mr.make_splits(size, self.chunk_mb)

        done = {int(c.split("-")[1]): c for c in self.zk.get_children(self.p_ckpt)}
        if done:
            self.log(f"продолжаю с чекпоинта: готовы задачи {sorted(done)} ({len(done)} из {len(splits)}), "
                     f"осталось {len(splits) - len(done)}")
        else:
            self.log(f"чекпоинта нет, начинаю с нуля: {len(splits)} map-задач по {self.chunk_mb} МиБ")

        for i, a, b in splits:
            if i in done:
                continue
            self.map_and_checkpoint(i, a, b, size, len(splits))

        totals, stats = self.reduce(len(splits))
        files = self.write_results(totals, stats)
        self.publish(files)

    # ------------------------------------------------------------------ map + чекпоинт
    def map_and_checkpoint(self, i, a, b, size, n):
        t0 = time.perf_counter()
        for attempt in range(1, mr.MAX_ATTEMPTS + 1):
            try:
                _, parts, st = mr.map_task_stream(i, a, b, size, -1, attempt, REDUCERS)
                break
            except Exception as e:                     # например, оборванное соединение с S3 после паузы
                self.log(f"map task {i}: ошибка на попытке {attempt}: {e!r}")
                if attempt == mr.MAX_ATTEMPTS:
                    raise
        # свой каталог эпохи: устаревший лидер не перезапишет файлы нового; действует тот, кто первым создал узел
        key = f"hw4/{self.run}/checkpoint/epoch-{self.epoch}/task-{i:02d}.json"
        body = json.dumps({"task": i, "stats": st, "parts": mr.parts_to_json(parts)}, ensure_ascii=False)
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"))
        info = json.dumps({"s3": key, "by": self.name, "epoch": self.epoch}).encode()
        node = f"{self.p_ckpt}/task-{i:02d}"
        try:
            self.zk.retry(self.zk.create, node, info)
            ready = len(self.zk.retry(self.zk.get_children, self.p_ckpt))
            self.log(f"map task {i:2d} готова за {time.perf_counter() - t0:.1f} с (строк {st['lines']}, "
                     f"вакансий {st.get('vacancies', 0)}); чекпоинт {ready}/{n}")
        except NodeExistsError:
            self.log(f"map task {i:2d} готова за {time.perf_counter() - t0:.1f} с, но {node} уже есть - "
                     f"её сделал другой координатор")

    # ------------------------------------------------------------------ reduce и сверка с ДЗ 3
    def reduce(self, n):
        t0 = time.perf_counter()
        all_parts, stats = [], []
        by = {}
        for i in range(n):
            info = json.loads(self.zk.retry(self.zk.get, f"{self.p_ckpt}/task-{i:02d}")[0])
            by[info["by"]] = by.get(info["by"], 0) + 1
            obj = json.loads(self.s3.get_object(Bucket=BUCKET, Key=info["s3"])["Body"].read())
            all_parts.append(mr.parts_from_json(obj["parts"]))
            stats.append(obj["stats"])
        totals = mr.reduce_all(all_parts)
        self.log(f"reduce по {n} задачам из чекпоинта (выполнили: {by}): {time.perf_counter() - t0:.1f} с")
        return totals, stats

    def write_results(self, totals, stats):
        lines = sum(s["lines"] for s in stats)
        vac = sum(s.get("vacancies", 0) for s in stats)
        ws = sum(s.get("with_skills", 0) for s in stats)
        self.log(f"строк прочитано мапперами: {lines} (данных {lines - 1}); вакансий варианта: {vac}; с навыками: {ws}")
        prefix = f"results/hw4/{self.run}/epoch-{self.epoch}/"
        files, all_ok = {}, True
        for k in mr.SIZES:
            body, rows = mr.to_csv(totals, k)
            name = mr.NAMES[k]
            self.s3.put_object(Bucket=BUCKET, Key=prefix + name, Body=body)
            mine = hashlib.sha256(body).hexdigest().upper()
            ref = hashlib.sha256(self.s3.get_object(Bucket=BUCKET, Key=HW3_PREFIX + name)["Body"].read()).hexdigest().upper()
            all_ok &= mine == ref
            files[name] = mine[:16]
            self.log(f"k={k}: уникальных ключей {len(rows)}, сумма частот {sum(totals[k].values())}; "
                     f"топ-3: {[[*key, v] for key, v in rows[:3]]}")
            self.log(f"  {name:15s} SHA-256 {mine[:16]}  ДЗ 3 {ref[:16]}  совпадает: {mine == ref}")
        self.log(f"контрольные числа и файлы {'СОВПАЛИ' if all_ok else 'НЕ СОВПАЛИ'} с ДЗ 3; файлы: s3://{BUCKET}/{prefix}")
        return {"prefix": prefix, "sha256": files, "match_hw3": all_ok}

    # ------------------------------------------------------------------ публикация
    def publish(self, files):
        payload = json.dumps({"epoch": self.epoch, "by": self.name, "pid": os.getpid(), **files,
                              "written_at": datetime.now().isoformat(timespec="milliseconds")},
                             ensure_ascii=False).encode()
        if FENCING:
            tx = self.zk.transaction()
            tx.check(self.p_epoch, self.epoch_version)
            tx.set_data(self.p_result, payload)
            res = self.zk.retry(tx.commit)
            errors = [r for r in res if isinstance(r, Exception)]
            if errors:
                cur, st = self.zk.get(self.p_epoch)
                self.log(f"публикация ОТКЛОНЕНА: {type(errors[0]).__name__} в check({self.p_epoch}, "
                         f"version={self.epoch_version}); сейчас эпоха {cur.decode()} (версия {st.version}), "
                         f"моя эпоха {self.epoch} устарела - указатель не изменён")
                self.exit_code = 3
            else:
                self.log(f"записал результат: эпоха {self.epoch}, {self.p_result} версия {res[1].version} "
                         f"(транзакция check(epoch, version={self.epoch_version}) + setData)")
        else:
            st = self.zk.retry(self.zk.set, self.p_result, payload)
            self.log(f"записал результат: эпоха {self.epoch}, {self.p_result} версия {st.version} (без проверки эпохи)")
        data, st = self.zk.get(self.p_result)
        d = json.loads(data)
        self.log(f"указатель {self.p_result}: версия {st.version}, эпоха {d['epoch']}, записал {d['by']}, "
                 f"{d['prefix']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True, help="имя копии координатора: A, B, ...")
    ap.add_argument("--run", required=True, help="имя запуска: отдельное пространство в ZooKeeper и S3")
    ap.add_argument("--chunk-mb", type=int, default=64)
    a = ap.parse_args()
    sys.exit(Coordinator(a.name, a.run, a.chunk_mb).run_forever())


if __name__ == "__main__":
    main()
