# 🕒 Python Job Scheduler mit TOML-Konfiguration

Dieser Job Scheduler startet Python-Skripte gemäß einer konfigurierbaren Zeitplanung in `.toml`-Dateien. Er unterstützt virtuelle Umgebungen (venv), Live-Reload bei Config-Änderungen und Logging in Datei und Konsole.

## 🚀 Features

- Zeitgesteuerte Ausführung von Scripts via `schedule`
- Konfigurierbar über TOML-Dateien (pro Job)
- Unterstützung für virtuelle Python-Umgebungen (venv)
- Live-Reload bei Config-Änderungen via `watchdog`
- Logging mit Rotation (Konsole + `logs/scheduler.log`)
- System-Tray-Icon (optional)
- Unterstützung für:
  - tägliche Ausführungen
  - mehrmals täglich (multi_daily)
  - wöchentliche Jobs (z. B. freitags 18:00)
  - monatliche Jobs
  - Intervalle wie `every_5_minutes`, `every_2_hours` etc.

## 📦 Installation

```bash
pip install -r requirements.txt
```

> Falls noch nicht vorhanden: Lege einen `config/`-Ordner an und darin eine `global.toml` und beliebige Job-Dateien (`jobname.toml`).

## 📁 Verzeichnisstruktur

```
projekt/
├── scheduler/
│   └── tray_icon.py        # Optionales Tray Icon
├── config/
│   ├── global.toml         # Globale Einstellungen
│   └── mein_job.toml       # Ein Job
├── logs/
│   └── scheduler.log       # Logfile (automatisch angelegt)
├── job_scheduler.py        # Hauptprogramm
└── requirements.txt
```

## ⚙️ Beispiel-Config für einen wöchentlichen Job

```toml
[metadata]
name = "woechentlicher_report"
enabled = true

[execution]
script = "scripts/weekly_report.py"
frequency = "weekly"
day = "friday"
time = "18:00"
venv = "venvs/report_env"
config = "configs/report_config.toml"
```

Die frequency kann "daily", "multi_daily", "weekly", "monthly" sein. Bei "multi_daily" musst du statt "time" ein Array von times mitgeben, z.B. ```times = ["12:00", "14:00"]```


## 🛠️ Globale Konfiguration (`config/global.toml`)

```toml
[global]
default_venv = "venvs/default_env"
```

Wenn kein `venv` im Job definiert ist, wird dieses als Fallback verwendet.

## ▶️ Starten (NICHT im VSCode - außerhalb in einer CMD-Shell)

```bash
python job_scheduler.py
```

alternativ
```bash
python -m scheduler.scheduler
```

## 🔄 Automatischer Reload

Wenn sich eine `.toml`-Datei im `config/`-Ordner ändert, wird der Scheduler den Job automatisch neu laden oder anlegen.

## 📌 Hinweise

- Der Arbeitsordner (`cwd`) für jedes Script wird automatisch auf dessen Speicherort gesetzt.
- Log-Ausgaben landen in `logs/scheduler.log` mit Rotation (max. 5 MB, 5 Dateien).
- Scripts sollten eigene relative Pfade möglichst mit `Path(__file__).parent` auflösen.

## 📋 Lizenz

MIT License – frei verwendbar mit Namensnennung.

---

🧑‍💻 Viel Spaß beim Automatisieren!
