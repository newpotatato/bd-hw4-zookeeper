# -*- coding: utf-8 -*-
"""Сборка отчёта ДЗ 4: report/report_src.html + выводы команд (outputs/, logs/) + скриншоты (screenshots/) -> PDF через Edge.

Метки в report_src.html:
  <!--OUT:файл:1-11,13-15-->   строки файла (путь от корня репозитория, напр. outputs/01_roles.txt) (нумерация с 1), диапазоны через запятую
  <!--IMG:имя.png|подпись-->     скриншот из screenshots/ (если файла нет - рамка-заглушка)
Запуск (PowerShell): python report\build.py
"""
import base64
import html
import pathlib
import re
import shutil
import subprocess
import tempfile
import time

SRC = pathlib.Path(__file__).resolve().parent
ROOT = SRC.parent
OUT_DIR, SHOT_DIR = ROOT / "outputs", ROOT / "screenshots"
PDF_NAME = "Черников_J3310_ДЗ4_ZooKeeper.pdf"
EDGE = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"


def out_block(m):
    name, ranges = m.group(1), m.group(2)
    lines = (ROOT / name).read_text(encoding="utf-8-sig").splitlines()
    picked = []
    for r in ranges.split(","):
        a, _, b = r.partition("-")
        a, b = int(a), int(b or a)
        if picked:
            picked.append("...")
        picked.extend(lines[a - 1:b])
    return "<pre>" + html.escape("\n".join(picked)) + "</pre>"


def img_block(m):
    name, caption = m.group(1), m.group(2)
    p = SHOT_DIR / name
    if p.exists():
        data = base64.b64encode(p.read_bytes()).decode()
        body = f'<img src="data:image/png;base64,{data}">'
    else:
        body = f'<div class="ph">Место для скриншота: screenshots/{name}</div>'
    return f'<figure>{body}<figcaption>{html.escape(caption)}</figcaption></figure>'


def main():
    s = (SRC / "report_src.html").read_text(encoding="utf-8")
    s = re.sub(r"<!--OUT:([^:]+):([0-9,\-]+)-->", out_block, s)
    s = re.sub(r"<!--IMG:([^|]+)\|([^>]*?)-->", img_block, s)
    missing = [n for n in re.findall(r"screenshots/([\w.]+)</div>", s)]
    out_html = ROOT / "report.html"
    out_html.write_text(s, encoding="utf-8")

    tmp = pathlib.Path(tempfile.mkdtemp(dir=ROOT, prefix="_pdf_"))  # не в AppData: Python из Store виртуализирует её
    pdf = tmp / "r.pdf"
    # при первом запуске Edge только создаёт профиль и может выйти без PDF - повторяем
    for _ in range(3):
        subprocess.run([EDGE, "--headless=new", "--disable-gpu", "--no-pdf-header-footer", "--no-first-run",
                        f"--user-data-dir={tmp / 'p'}", f"--print-to-pdf={pdf}", out_html.as_uri()],
                       check=True, timeout=120, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # msedge.exe может вернуть управление сразу, а PDF дописать позже - ждём файл и стабильный размер
        last = -1
        for _ in range(60):
            time.sleep(1)
            if pdf.exists() and pdf.stat().st_size == last > 0:
                break
            last = pdf.stat().st_size if pdf.exists() else -1
        if pdf.exists():
            break
    shutil.copy(pdf, ROOT / PDF_NAME)
    shutil.rmtree(tmp, ignore_errors=True)
    from pypdf import PdfReader
    print(f"PDF: {PDF_NAME}, страниц: {len(PdfReader(ROOT / PDF_NAME).pages)}")
    if missing:
        print("нет скриншотов:", ", ".join(missing))


if __name__ == "__main__":
    main()
