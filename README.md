# ДЗ 4: отказоустойчивость средствами Apache ZooKeeper

Черников Кирилл Михайлович, группа J3310, вариант 17 — «Программист, разработчик на C# и .NET».

Отчёт: [`Черников_J3310_ДЗ4_ZooKeeper.pdf`](Черников_J3310_ДЗ4_ZooKeeper.pdf)

## Что сделано
- Ансамбль из трёх серверов ZooKeeper 3.9.6 в Docker (Java 17 внутри образа): у каждого свой `zoo.cfg`, порты и `dataDir`, файл `myid` в `dataDir`.
- Роли серверов; остановка лидера — ансамбль выбрал нового и принял запись; остановка второго — оставшийся сервер не обслуживает клиентов (`not currently serving requests`, `ConnectionLoss`); запуск обоих — отставший сервер догнал запись.
- Эфемерный узел из zkCli во втором окне: после закрытия окна узел исчез через 8,1 с (по истечении сессии 10 с).
- Координатор расчёта пар навыков из ДЗ 3 с выбором лидера, эпохой и чекпоинтом в ZooKeeper; две копии, тайм-аут сессии 10 с, лидер приостановлен на 40 с после 5 из 13 map-задач.

| | Без защиты (`FENCING=0`) | С защитой (`FENCING=1`) |
|---|---|---|
| Запасной B стал лидером после приостановки A | 11,91 с | 11,28 с |
| B продолжил с чекпоинта | задачи 0–4 готовы, посчитал 5–12 | то же |
| Результат совпал с ДЗ 3 | да: 4356 / 3345 вакансий, SHA-256 всех трёх CSV | да |
| Публикации | две строки «записал результат» | одна; у A `BadVersionError` |
| Указатель `/hw4/<run>/result` | `dataVersion = 2`, эпоха 1, записал A | `dataVersion = 1`, эпоха 2, записал B |

## Структура
| Путь | Назначение |
|---|---|
| `compose.yaml` | три сервера ZooKeeper, запуск `zkServer.sh start-foreground` |
| `zk/zkN/zoo.cfg`, `zk/zkN/data/myid` | конфигурация и номер сервера N |
| `src/coordinator.py` | координатор: выборы (kazoo Election), эпоха, чекпоинт, публикация с проверкой эпохи |
| `src/skills_mr.py`, `src/common.py` | map-функция и настройки из ДЗ 3 |
| `src/scenario.py` | этапы 3–4: две копии координатора, приостановка лидера, замер смены лидера |
| `scripts/stage.ps1` | все этапы по одному, вывод на экран и в `outputs/` |
| `scripts/ephemeral.py`, `scripts/window2_zkcli.cmd` | опыт с эфемерным узлом (два окна) |
| `scripts/shoot.ps1`, `scripts/winshot.ps1` | скриншоты окон терминала |
| `logs/` | журналы координаторов и сценария: `nofence_*` (этап 3), `fencing_*` (этап 4) |
| `outputs/` | выводы команд текстом, `01`–`09` по порядку этапов |
| `screenshots/` | скриншоты, вошедшие в отчёт |
| `report/` | исходник отчёта и сборка PDF |

## Запуск
Нужен RustFS с датасетом и результатами ДЗ 3 (репозиторий `bd-hw3-mapreduce`, бакет `hh-lake`).
```powershell
docker compose up -d
python -m venv .venv; .\.venv\Scripts\pip install kazoo boto3 psutil pypdf
foreach ($s in "roles","one_down","two_down","restart","ephemeral") { .\scripts\stage.ps1 -Stage $s }
.\.venv\Scripts\python.exe src\scenario.py --run nofence --fencing 0   # этап 3
.\.venv\Scripts\python.exe src\scenario.py --run fencing --fencing 1   # этап 4
.\.venv\Scripts\python.exe report\build.py                             # PDF отчёта
```

## Замечания
- В `logs/` только два финальных запуска, отладочные удалены.
- В `outputs/06_run_nofence.txt` при записи файла пропала одна итоговая строка про указатель (`dataVersion = 2`). Она есть в `logs/nofence_scenario.log` и `outputs/08_check_nofence.txt`. Причина — кратковременная блокировка файла; запись в `stage.ps1` после этого сделана с повтором.
- В выводах zkCli убраны служебные строки журнала уровня INFO и строки стека Java.
