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

### `working_dir` — optionales Arbeitsverzeichnis

Standardmäßig wird das Verzeichnis des Scripts (`script_path.parent`) als Arbeitsverzeichnis (`cwd`) verwendet.
Mit dem optionalen Key `working_dir` kann ein abweichendes Verzeichnis gesetzt werden — z. B. das Projekt-Root, wenn das Script Imports aus übergeordneten Paketen benötigt:

```toml
[execution]
script = "SKVU_US/us_health_monitor.py"
working_dir = "/opt/myproject"   # optional; default: Verzeichnis des Scripts
frequency = "daily"
time = "06:00"
```

### Automatisches PYTHONPATH-Handling

Vor dem Start jedes Scripts werden **zwei Einträge automatisch** an den Anfang von `PYTHONPATH` gestellt — ohne dass die Job-Config angefasst werden muss:

1. **Effektives Arbeitsverzeichnis** (`working_dir` falls gesetzt, sonst `script_path.parent`)
2. **Erkannter Projekt-Root** des Scripts — der Scheduler sucht in den Elternverzeichnissen des Scripts nach dem ersten Verzeichnis, das einen der folgenden Marker enthält: `.git`, `pyproject.toml`, `setup.py`, `setup.cfg`, `requirements.txt`. Wird ein solcher Root gefunden, wird er ebenfalls in `PYTHONPATH` aufgenommen.

**Beispiel:** Script liegt in `atlassian_utils/SKVU_US/us_health_monitor.py`, abhängiges Package in `atlassian_utils/WstwConfl/`. Da `atlassian_utils/` ein `.git`-Verzeichnis enthält, wird `atlassian_utils/` automatisch erkannt und in `PYTHONPATH` eingetragen — `from WstwConfl import ...` funktioniert ohne weitere Config.

Weitere Regeln:

- Ist `PYTHONPATH` bereits gesetzt, werden die neuen Einträge vorangestellt (`:` auf macOS/Linux, `;` auf Windows).
- Einträge, die bereits in `PYTHONPATH` enthalten sind, werden nicht doppelt eingetragen.
- Wird kein Projekt-Marker gefunden, werden keine weiteren Elternverzeichnisse hinzugefügt.
- Ist `script_path` relativ, wird er für die Root-Erkennung gegen `working_dir` (falls gesetzt) oder das aktuelle Prozess-cwd aufgelöst.


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

- Der Arbeitsordner (`cwd`) für jedes Script wird standardmäßig auf das Verzeichnis des Scripts gesetzt; mit dem optionalen Key `working_dir` kann ein abweichendes Verzeichnis verwendet werden.
- `PYTHONPATH` wird immer automatisch um das effektive Arbeitsverzeichnis **und** den erkannten Projekt-Root des Scripts erweitert — kein manuelles Setzen pro Job nötig.
- Log-Ausgaben landen in `logs/scheduler.log` mit Rotation (max. 5 MB, 5 Dateien).
- Scripts sollten eigene relative Pfade möglichst mit `Path(__file__).parent` auflösen.

## 📋 Lizenz

MIT License – frei verwendbar mit Namensnennung.

---

🧑‍💻 Viel Spaß beim Automatisieren!
