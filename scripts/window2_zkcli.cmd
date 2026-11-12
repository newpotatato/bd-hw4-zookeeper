@echo off
rem Второе окно: интерактивный клиент zkCli (сессия 10 с) создаёт эфемерный узел и остаётся подключённым.
rem Команды подаются в stdin клиента; ping держит stdin открытым, пока окно не закроют.
chcp 65001 >nul
title hw4-window2
echo [window 2] zkCli.sh -timeout 10000 -server zk1:2181,zk2:2182,zk3:2183
echo [window 2] ^> create -e /hw4-ephemeral created-from-window-2
echo [window 2] ^> stat /hw4-ephemeral
(echo create -e /hw4-ephemeral created-from-window-2& echo stat /hw4-ephemeral& ping -n 3600 127.0.0.1 >nul) | docker run --rm -i --name zkcli-window2 --network hw4_default zookeeper:3.9.6 zkCli.sh -timeout 10000 -server zk1:2181,zk2:2182,zk3:2183
