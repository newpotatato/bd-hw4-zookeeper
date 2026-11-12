# -*- coding: utf-8 -*-
"""Этапы 3 и 4: две копии координатора, приостановка лидера посреди расчёта, замер смены лидера.

1. Очищает пространство запуска (/hw4/<run> в ZooKeeper, hw4/<run>/ и results/hw4/<run>/ в S3).
2. Запускает координатор A, дожидается его лидерства, запускает запасной координатор B.
3. Когда в чекпоинте --pause-after задач, приостанавливает процесс A целиком (psutil suspend -
   аналог SIGSTOP: замирают все потоки, включая пинги ZooKeeper, - так выглядит долгая пауза GC или зависание).
4. Засекает, когда эфемерный узел A исчез из очереди выборов и когда B написал «СТАЛ ЛИДЕРОМ».
5. Ждёт окончания работы B (но не меньше --pause секунд), возобновляет A и ждёт его окончания.
6. Печатает строки публикации из журналов обоих координаторов и итоговое состояние указателя.

Пример: python src/scenario.py --run nofence --fencing 0
"""
import argparse
import json
import os
import pathlib
import re
import subprocess
import sys
import time
from datetime import datetime

import psutil
from kazoo.client import KazooClient

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from common import BUCKET, s3_client  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parent.parent
ZK_HOSTS = os.getenv("ZK_HOSTS", "localhost:2181,localhost:2182,localhost:2183")
LOGS = ROOT / "logs"


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


class Scenario:
    def __init__(self, a):
        self.a = a
        self.base = f"/hw4/{a.run}"
        LOGS.mkdir(exist_ok=True)
        self.logf = open(LOGS / f"{a.run}_scenario.log", "w", encoding="utf-8")
        self.zk = KazooClient(hosts=ZK_HOSTS, timeout=10.0)
        self.s3 = s3_client()

    def say(self, msg):
        line = f"{now()} [scenario] {msg}"
        print(line, flush=True)
        self.logf.write(line + "\n")
        self.logf.flush()

    def wait(self, cond, what, timeout=120, step=0.05):
        t0 = time.time()
        while not cond():
            if time.time() - t0 > timeout:
                raise TimeoutError(what)
            time.sleep(step)

    # ------------------------------------------------------------------
    def reset(self):
        if self.zk.exists(self.base):
            self.zk.delete(self.base, recursive=True)
        for prefix in (f"hw4/{self.a.run}/", f"results/hw4/{self.a.run}/"):
            keys = [o["Key"] for o in self.s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])]
            if keys:
                self.s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in keys]})
        self.say(f"очищено: {self.base} в ZooKeeper, hw4/{self.a.run}/ и results/hw4/{self.a.run}/ в S3")

    def start(self, name):
        env = dict(os.environ, FENCING=str(self.a.fencing), SESSION_TIMEOUT="10",
                   PYTHONIOENCODING="utf-8", PYTHONUNBUFFERED="1")
        p = subprocess.Popen([sys.executable, str(ROOT / "src" / "coordinator.py"), "--name", name, "--run", self.a.run],
                             env=env, cwd=ROOT)
        self.say(f"запущен координатор {name}: pid {p.pid}, FENCING={self.a.fencing}, тайм-аут сессии 10 с")
        return p

    def contenders(self):
        try:
            return self.election.contenders()
        except Exception:
            return []

    def ckpt_count(self):
        p = f"{self.base}/checkpoint"
        return len(self.zk.get_children(p)) if self.zk.exists(p) else 0

    @staticmethod
    def log_time(name_log, pattern):
        """Время первой строки журнала координатора, содержащей pattern."""
        for line in (LOGS / name_log).read_text(encoding="utf-8").splitlines():
            if pattern in line:
                return datetime.strptime(line[:23], "%Y-%m-%d %H:%M:%S.%f").timestamp(), line
        return None, None

    # ------------------------------------------------------------------
    def run(self):
        a = self.a
        self.zk.start()
        self.reset()
        self.election = self.zk.Election(f"{self.base}/election")

        pa = self.start("A")
        self.wait(lambda: self.contenders()[:1] == ["A"], "A не стал лидером")
        pb = self.start("B")
        self.wait(lambda: "B" in self.contenders(), "B не встал в очередь")
        self.say(f"очередь выборов: {self.contenders()} - лидер A, запасной B ждёт")

        self.wait(lambda: self.ckpt_count() >= a.pause_after, "нет прогресса чекпоинта", timeout=180)
        # python.exe из venv на Windows - лаунчер, сам интерпретатор - его дочерний процесс: замораживаем всё дерево
        root = psutil.Process(pa.pid)
        tree = [root] + root.children(recursive=True)
        for pr in tree:
            pr.suspend()
        t_pause = time.time()
        self.say(f"ПРИОСТАНОВИЛ лидера A (процессы {[pr.pid for pr in tree]}, psutil suspend) посреди расчёта: "
                 f"в чекпоинте {self.ckpt_count()} из 13 задач")

        t_gone = t_view = None
        while t_view is None:
            c = self.contenders()
            if t_gone is None and "A" not in c:
                t_gone = time.time()
                self.say(f"+{t_gone - t_pause:.2f} с: узел A исчез из очереди выборов (сервер признал сессию A истёкшей)")
            if c[:1] == ["B"]:
                t_view = time.time()
            time.sleep(0.05)
        self.wait(lambda: self.log_time("%s_B.log" % a.run, "СТАЛ ЛИДЕРОМ")[0] is not None, "B не стал лидером")
        t_b, _ = self.log_time("%s_B.log" % a.run, "СТАЛ ЛИДЕРОМ")
        self.say(f"+{t_b - t_pause:.2f} с: B написал «СТАЛ ЛИДЕРОМ» - смена лидера заняла {t_b - t_pause:.2f} с")

        pb.wait(timeout=300)
        self.say(f"запасной координатор B завершился (код {pb.returncode}) через {time.time() - t_pause:.1f} с после паузы A")
        rest = a.pause - (time.time() - t_pause)
        if rest > 0:
            time.sleep(rest)
        for pr in tree:
            pr.resume()
        t_resume = time.time()
        self.say(f"ВОЗОБНОВИЛ A после паузы {t_resume - t_pause:.1f} с")
        pa.wait(timeout=300)
        self.say(f"координатор A завершился (код {pa.returncode})")

        self.summary(t_pause, t_gone, t_b)
        self.zk.stop()

    def summary(self, t_pause, t_gone, t_b):
        a = self.a
        self.say("=" * 30 + f" ИТОГ запуска {a.run} (FENCING={a.fencing}) " + "=" * 30)
        self.say(f"время смены лидера: узел A удалён через {t_gone - t_pause:.2f} с, "
                 f"B стал лидером через {t_b - t_pause:.2f} с после приостановки A (тайм-аут сессии 10 с)")
        for name in ("A", "B"):
            lines = (LOGS / f"{a.run}_{name}.log").read_text(encoding="utf-8").splitlines()
            for line in lines:
                if re.search(r"СТАЛ ЛИДЕРОМ|продолжаю с чекпоинта|reduce по|записал результат|ОТКЛОНЕНА|СОВПАЛИ|LOST", line):
                    self.say(f"  {line[24:]}")
            dup = sum("уже есть" in line for line in lines)
            if dup:
                self.say(f"  [{name}] после возобновления пересчитал {dup} задач, которые уже сделал B "
                         f"(узлы чекпоинта заняты - его выход не используется)")
        data, st = self.zk.get(f"{self.base}/result")
        d = json.loads(data)
        self.say(f"указатель {self.base}/result: dataVersion = {st.version}, эпоха {d['epoch']}, записал {d['by']}, "
                 f"{d['prefix']}, совпадает с ДЗ 3: {d['match_hw3']}")
        ep, est = self.zk.get(f"{self.base}/epoch")
        self.say(f"узел {self.base}/epoch: {ep.decode()} (dataVersion = {est.version})")
        prefixes = sorted({o["Key"].rsplit("/", 1)[0] for o in
                           self.s3.list_objects_v2(Bucket=BUCKET, Prefix=f"results/hw4/{a.run}/").get("Contents", [])})
        self.say(f"каталоги результатов в S3: {prefixes}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--fencing", type=int, choices=[0, 1], default=0)
    ap.add_argument("--pause-after", type=int, default=5, help="приостановить A, когда в чекпоинте столько задач")
    ap.add_argument("--pause", type=float, default=40, help="минимальная длительность паузы A, с")
    Scenario(ap.parse_args()).run()


if __name__ == "__main__":
    main()
