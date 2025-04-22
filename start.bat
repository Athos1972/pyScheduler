@echo off
title Python Job Scheduler
cd /d %~dp0
python -m scheduler.scheduler
pause