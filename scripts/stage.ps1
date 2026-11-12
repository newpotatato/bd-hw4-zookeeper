# Этапы ДЗ 4 по одному: вывод идёт на экран и в outputs\<файл>.txt (выводы команд текстом для отчёта).
# Пример: powershell -File scripts\stage.ps1 -Stage roles
# Для скриншотов scripts\shoot.ps1 запускает этот же скрипт в отдельном окне.
param([string]$Stage, [switch]$Hold)
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"
$Host.UI.RawUI.WindowTitle = "hw4-$Stage"
try { $Host.UI.RawUI.BufferSize = New-Object Management.Automation.Host.Size(190, 3000) } catch {}
Set-Location "$PSScriptRoot\.."
$py = ".\.venv\Scripts\python.exe"
$ALL = "zk1:2181,zk2:2182,zk3:2183"
$files = @{ roles = "01_roles.txt"; one_down = "02_one_down.txt"; two_down = "03_two_down.txt";
            restart = "04_restart.txt"; ephemeral = "05_ephemeral.txt";
            nofence = "06_run_nofence.txt"; fencing = "07_run_fencing.txt";
            check_nofence = "08_check_nofence.txt"; check_fencing = "09_check_fencing.txt" }
$out = "outputs\$($files[$Stage])"
New-Item -ItemType Directory -Force outputs | Out-Null
Set-Content -Path $out -Value "" -Encoding utf8 -NoNewline

function Say($text, $color = "Gray") {
    Write-Host $text -ForegroundColor $color
    # файл бывает кратко заблокирован (антивирус) - повторяем запись, чтобы строка не потерялась
    for ($k = 0; $k -lt 50; $k++) {
        try { [IO.File]::AppendAllText((Join-Path (Get-Location) $out), "$text`r`n", [Text.Encoding]::UTF8); break }
        catch { Start-Sleep -Milliseconds 100 }
    }
}
# Печатает команду, выполняет её и печатает вывод (stderr тоже); шумные строки журнала zkCli убираются
function Run([string]$shown, [scriptblock]$sb) {
    Say "PS> $shown" Yellow
    $lines = & { $ErrorActionPreference = "Continue"; & $sb 2>&1 } | ForEach-Object { "$_" }
    foreach ($l in $lines) {
        if ($l -match "^\d{4}-\d\d-\d\d .* - INFO " -or $l -match "^(WATCHER::|WatchedEvent state:SyncConnected)" -or
            $l -match "^ZooKeeper JMX enabled" -or $l -match "^\s+at " -or $l -eq "") { continue }
        Say $l
    }
}
function Status($i) {
    Run "docker exec zk$i zkServer.sh status" { docker exec "zk$i" zkServer.sh status }.GetNewClosure()
}
function ShowPs { Run "docker compose ps -a" { docker compose ps -a --format "table {{.Name}}	{{.Status}}" } }
function Running { 1..3 | Where-Object { (docker inspect -f "{{.State.Running}}" "zk$_" 2>$null) -eq "true" } }
function ZkCli($server, [string[]]$cmd) {
    # клиент - отдельный контейнер в сети ансамбля (как отдельная машина), не зависит от того, какие серверы остановлены
    $shown = "zkCli.sh -server $server " + ($cmd -join " ")
    # timeout: без кворума клиент бесконечно переподключается, ограничиваем 15 с
    Run $shown { docker run --rm --network hw4_default --entrypoint timeout zookeeper:3.9.6 15 zkCli.sh -server $server @cmd
                 if ($LASTEXITCODE -eq 124) { "(нет ответа за 15 с, код выхода 124)" } }.GetNewClosure()
}
function Stamp { Say ("# " + (Get-Date -Format "yyyy-MM-dd HH:mm:ss")) DarkGray }

switch ($Stage) {
    "roles" {
        Stamp
        Run "docker compose ps" { docker compose ps --format "table {{.Name}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}" }
        Run "Get-Content zk\zk1\data\myid, zk\zk2\data\myid, zk\zk3\data\myid" { foreach ($i in 1..3) { "zk$i/data/myid = " + (Get-Content "zk\zk$i\data\myid") } }
        foreach ($i in 1..3) { Status $i }
        Run "docker exec zk1 java -version" { docker exec zk1 java -version }
    }
    "one_down" {
        # останавливаем текущего лидера ансамбля - самый неприятный случай
        $leader = (1..3 | Where-Object { (docker exec "zk$_" zkServer.sh status 2>&1 | Out-String) -match "Mode: leader" })[0]
        Stamp
        Say "# лидер ансамбля сейчас zk$leader - останавливаем его" Cyan
        Run "docker compose stop zk$leader" { docker compose stop "zk$leader" }
        Start-Sleep -Seconds 3
        ShowPs
        foreach ($i in Running) { Status $i }
        $live = @(Running)
        Say "# пишем через zk$($live[0]), читаем через zk$($live[1])" Cyan
        ZkCli "zk$($live[0]):218$($live[0])" @("create", "/hw4-check", "written-with-2-of-3")
        ZkCli "zk$($live[1]):218$($live[1])" @("get", "-s", "/hw4-check")
        Set-Content "outputs\.stopped1" $leader
    }
    "two_down" {
        $first = Get-Content "outputs\.stopped1"
        $alive = 1..3 | Where-Object { "$_" -ne "$first" }
        $second = $alive[1]; $last = $alive[0]
        Stamp
        Say "# zk$first уже остановлен; останавливаем zk$second, остаётся один zk$last" Cyan
        Run "docker compose stop zk$second" { docker compose stop "zk$second" }
        Start-Sleep -Seconds 3
        ShowPs
        foreach ($i in Running) { Status $i }
        Run "docker exec zk$last sh -c 'echo srvr | nc localhost 218$last'" { docker exec "zk$last" sh -c "echo srvr | nc localhost 218$last" }
        ZkCli "zk$($last):218$last" @("get", "/hw4-check")
        Set-Content "outputs\.stopped2" $second
    }
    "restart" {
        $a = Get-Content "outputs\.stopped1"; $b = Get-Content "outputs\.stopped2"
        Stamp
        Run "docker compose start zk$a zk$b" { docker compose start "zk$a" "zk$b" }
        Start-Sleep -Seconds 6
        ShowPs
        foreach ($i in 1..3) { Status $i }
        Say "# zk$a был остановлен во время записи /hw4-check - после запуска он получил её от лидера" Cyan
        ZkCli "zk$($a):218$a" @("get", "/hw4-check")
        ZkCli $ALL @("delete", "/hw4-check")
    }
    "ephemeral" {
        & $py scripts\ephemeral.py | ForEach-Object { Say $_ }
    }
    "nofence" {
        & $py src\scenario.py --run nofence --fencing 0 | ForEach-Object { Say $_ }
    }
    "fencing" {
        & $py src\scenario.py --run fencing --fencing 1 | ForEach-Object { Say $_ }
    }
    { $_ -in "check_nofence", "check_fencing" } {
        $run = $Stage.Substring(6)
        # дата у всех строк одна - убираем её, чтобы строки помещались в окно
        Run "Get-Content logs\${run}_scenario.log | Select-String 'ПРИОСТАНОВИЛ|исчез|СТАЛ ЛИДЕРОМ|ВОЗОБНОВИЛ|время смены|dataVersion'" {
            Get-Content "logs\${run}_scenario.log" -Encoding utf8 | Where-Object { $_ -match "ПРИОСТАНОВИЛ|исчез|«СТАЛ ЛИДЕРОМ»|ВОЗОБНОВИЛ|время смены|dataVersion" } |
                ForEach-Object { $_.Substring(11) } }.GetNewClosure()
        Run "Select-String 'СТАЛ ЛИДЕРОМ|продолжаю с чекпоинта|СОВПАЛИ|записал результат|ОТКЛОНЕНА' logs\${run}_A.log, logs\${run}_B.log" {
            foreach ($n in "A", "B") {
                Get-Content "logs\${run}_$n.log" -Encoding utf8 | Where-Object { $_ -match "СТАЛ ЛИДЕРОМ|продолжаю с чекпоинта|СОВПАЛИ|записал результат|ОТКЛОНЕНА" } |
                    ForEach-Object { "${run}_$n.log: " + $_.Substring(11) } } }.GetNewClosure()
        ZkCli $ALL @("get", "-s", "/hw4/$run/result")
        ZkCli $ALL @("get", "/hw4/$run/epoch")
    }
}
if ($Hold) {
    New-Item -ItemType File -Force "outputs\_done_$Stage" | Out-Null
    Start-Sleep -Seconds 900
}
