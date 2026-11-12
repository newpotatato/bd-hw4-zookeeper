# -*- coding: utf-8 -*-
"""Этап 2, эфемерный узел: zkCli во втором окне создаёт /hw4-ephemeral, окно закрывается, узел исчезает.

Первое окно (этот скрипт) открывает второе окно (scripts/window2_zkcli.cmd), ждёт появления узла,
показывает его через zkCli (ephemeralOwner = номер сессии второго окна), снимает второе окно,
закрывает его вместе с клиентом и раз в секунду проверяет, есть ли ещё узел.
"""
import pathlib
import subprocess
import sys
import time
from datetime import datetime

from kazoo.client import KazooClient

ROOT = pathlib.Path(__file__).resolve().parent.parent
HOSTS = "localhost:2181,localhost:2182,localhost:2183"
NODE = "/hw4-ephemeral"


def say(*m):
    print(*m, flush=True)


def ts():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def zkcli(*cmd):
    """zkCli в отдельном контейнере - клиент первого окна."""
    say("PS> zkCli.sh -server zk1:2181,zk2:2182,zk3:2183 " + " ".join(cmd))
    r = subprocess.run(["docker", "run", "--rm", "--network", "hw4_default", "zookeeper:3.9.6", "zkCli.sh",
                        "-server", "zk1:2181,zk2:2182,zk3:2183", *cmd],
                       capture_output=True, text=True, encoding="utf-8", errors="replace")
    for line in (r.stdout + r.stderr).splitlines():
        if " - INFO " in line or line.startswith(("WATCHER::", "WatchedEvent state:SyncConnected")) or not line.strip():
            continue
        say(line)


def main():
    zk = KazooClient(hosts=HOSTS, timeout=10.0)
    zk.start()
    if zk.exists(NODE):
        zk.delete(NODE)

    say(f"# {ts()} открываю второе окно с zkCli (scripts/window2_zkcli.cmd)")
    win = subprocess.Popen(["conhost.exe", "cmd.exe", "/c", str(ROOT / "scripts" / "window2_zkcli.cmd")],
                           creationflags=subprocess.CREATE_NEW_CONSOLE)
    t0 = time.time()
    while not zk.exists(NODE):
        if time.time() - t0 > 60:
            sys.exit("узел не появился за 60 с")
        time.sleep(0.2)
    data, st = zk.get(NODE)
    say(f"# {ts()} узел {NODE} создан из второго окна: data={data.decode()!r}, ephemeralOwner=0x{st.ephemeralOwner:x}")
    time.sleep(2)
    zkcli("get", "-s", NODE)

    shot = ROOT / "screenshots" / "05a_window2.png"
    subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                    str(ROOT / "scripts" / "winshot.ps1"), "-Title", "hw4-window2", "-Out", str(shot)],
                   capture_output=True)

    owner = st.ephemeralOwner
    # закрыть окно = завершить консоль второго окна вместе со всеми её процессами (cmd, ping, docker-клиент)
    subprocess.run(["taskkill", "/T", "/F", "/PID", str(win.pid)], capture_output=True)
    t_close = time.time()
    say(f"# {ts()} второе окно закрыто (taskkill /T /F /PID {win.pid})")
    while True:
        st = zk.exists(NODE)
        dt = time.time() - t_close
        if st is None:
            say(f"# {ts()} +{dt:4.1f} с: узла {NODE} больше нет")
            break
        say(f"# {ts()} +{dt:4.1f} с: узел ещё есть, ephemeralOwner=0x{st.ephemeralOwner:x}")
        if dt > 90:
            sys.exit("узел не исчез за 90 с")
        time.sleep(1)
    ps = subprocess.run(["docker", "ps", "-a", "--filter", "name=zkcli-window2", "--format", "{{.Names}} {{.Status}}"],
                        capture_output=True, text=True).stdout.strip()
    say(f"# контейнер клиента второго окна: {ps or 'завершён и удалён'}")
    say(f"# узел жил после закрытия окна {dt:.1f} с - пока сервер не признал сессию 0x{owner:x} истёкшей")
    zkcli("ls", "/")
    zkcli("stat", NODE)
    zk.stop()


if __name__ == "__main__":
    main()
