import schedule
"""
JobScheduler
Ein Scheduler zur Verwaltung und Ausführung von geplanten Aufgaben basierend auf Konfigurationsdateien im TOML-Format. Mehr Details im Readme.

"""
import time
import subprocess
import toml
import os
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime, timedelta
from scheduler.tray_icon import setup_tray_icon
from typing import Optional, Dict, Any


class JobScheduler:
    """
    JobScheduler-Klasse
    Die JobScheduler-Klasse dient zur Verwaltung und Ausführung von geplanten Aufgaben (Jobs). 
    Sie bietet Funktionen zum Laden, Hinzufügen, Aktualisieren und Ausführen von Jobs basierend 
    auf Konfigurationsdateien im TOML-Format. Die Klasse unterstützt verschiedene Frequenzen 
    für die Ausführung von Jobs, darunter täglich, wöchentlich, monatlich und benutzerdefinierte Intervalle.
    Attribute:
    -----------
    config_dir : Path
        Verzeichnis, in dem die Konfigurationsdateien gespeichert sind.
    jobs : dict
        Enthält Informationen zu allen geladenen Jobs, einschließlich Konfiguration, 
        letzter Ausführung und nächster geplanter Ausführung.
    default_venv : str oder None
        Standard-Virtual-Environment, das für die Ausführung von Jobs verwendet wird.
    logger : logging.Logger
        Logger-Instanz für die Protokollierung von Ereignissen und Fehlern.
    observer : Observer
        Überwacht Änderungen an den Konfigurationsdateien im config_dir.
    Methoden:
    ---------
    __init__(config_dir="config"):
        Initialisiert den Scheduler, lädt die globale Konfiguration und startet den Dateiüberwacher.
    _setup_logging():
        Richtet die Protokollierung ein, einschließlich Datei- und Konsolenausgabe.
    _init_file_watcher():
        Startet einen Dateiüberwacher, der Änderungen an den Konfigurationsdateien erkennt.
    _handle_config_change(config_path):
        Wird aufgerufen, wenn eine Konfigurationsdatei geändert wird, und lädt die Änderungen.
    _load_global_config():
        Lädt die globale Konfiguration aus der Datei "global.toml".
    load_jobs():
        Lädt alle Jobs aus den Konfigurationsdateien im config_dir.
    add_job(config_path):
        Fügt einen neuen Job basierend auf einer Konfigurationsdatei hinzu.
    _update_job(job_name, new_config):
        Aktualisiert einen bestehenden Job mit einer neuen Konfiguration.
    _schedule_job(config):
        Plant einen Job basierend auf der angegebenen Konfiguration.
    _calculate_next_run(time_str, day=None):
        Berechnet den nächsten Ausführungszeitpunkt für tägliche oder wöchentliche Jobs.
    _calculate_next_monthly_run(day, time_str):
        Berechnet den nächsten Ausführungszeitpunkt für monatliche Jobs.
    _create_job_func(exec_cfg, job_name):
        Erstellt eine Funktion, die den Job ausführt.
    _run_script(script_path, config_path, venv_path=None):
        Führt das angegebene Skript mit optionaler Konfiguration und Virtual-Environment aus.
    run_pending():
        Startet den Scheduler und führt alle anstehenden Jobs aus.
    _log_upcoming_jobs():
        Protokolliert alle Jobs, die in den nächsten 24 Stunden ausgeführt werden.
    _format_timedelta(seconds):
        Formatiert eine Zeitspanne in Stunden und Minuten.
    """
    
    def __init__(self, config_dir: str = "config") -> None:
        """
        Initialisiert den Job Scheduler.
        Args:
            config_dir (str): Der Pfad zum Konfigurationsverzeichnis. Standardmäßig "config".
        Attribute:
            config_dir (Path): Der Pfad zum Konfigurationsverzeichnis als Path-Objekt.
            jobs (Dict[str, Dict[str, Any]]): Ein DICT, das die konfigurierten Jobs speichert.
            default_venv (Optional[str]): Der Standard-Python-Virtual-Environment-Pfad, falls gesetzt.
            logger (logging.Logger): Der Logger für den Scheduler.
        Beschreibung:
            - Initialisiert die Dateiüberwachung.
            - Lädt die globale Konfiguration.
            - Loggt Initialisierungsinformationen wie das Konfigurationsverzeichnis und das Standard-VENV.
        """
        self.config_dir: Path = Path(config_dir)
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.default_venv: Optional[str] = None
        self.logger: logging.Logger = self._setup_logging()
        self._init_file_watcher()
        self._load_global_config()
        
        self.logger.info("="*50)
        self.logger.info("Initializing Job Scheduler")
        self.logger.info(f"Config directory: {self.config_dir.absolute()}")
        self.logger.info(f"Default VENV: {self.default_venv or 'Not set'}")
        self.logger.info("="*50)

    def _setup_logging(self) -> logging.Logger:
        """
        Richtet die Protokollierung (Logging) für die Anwendung ein.
        Erstellt ein Verzeichnis namens "logs", falls es nicht existiert, und konfiguriert
        einen RotatingFileHandler, um Protokolldateien mit einer maximalen Größe von 5 MB
        und bis zu 5 Sicherungskopien zu verwalten. Die Protokollausgaben enthalten
        Zeitstempel, Protokollebene und Nachrichteninhalt.
        Zusätzlich wird ein StreamHandler eingerichtet, um Protokollnachrichten auch
        auf der Konsole auszugeben.
        Rückgabewert:
            logging.Logger: Der konfigurierte Logger für die Anwendung.
        """

        Path("logs").mkdir(exist_ok=True)
        handler = RotatingFileHandler(
            'logs/scheduler.log',
            maxBytes=5*1024*1024,  # 5 MB
            backupCount=5,
            encoding='utf-8'
        )
        logging.basicConfig(
            handlers=[handler],
            level=logging.INFO,
            format='%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Auch auf der Konsole ausgeben
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s'))
        logging.getLogger().addHandler(console_handler)
        logging.getLogger().setLevel(logging.INFO)

        return logging.getLogger()

    def _init_file_watcher(self) -> None:
        """
        Initialisiert einen Dateiüberwacher, der Änderungen an Konfigurationsdateien überwacht.
        Diese Methode erstellt eine Unterklasse von `FileSystemEventHandler`, um auf Änderungen
        an `.toml`-Dateien zu reagieren. Wenn eine solche Datei geändert wird, wird die Methode
        `_handle_config_change` des Schedulers aufgerufen.
        Der Dateiüberwacher wird mit dem angegebenen Konfigurationsverzeichnis (`self.config_dir`)
        initialisiert und gestartet.
        Klassen:
            ConfigHandler: Eine Unterklasse von `FileSystemEventHandler`, die auf Änderungen
            an `.toml`-Dateien reagiert.
        Attribute:
            observer (Observer): Ein Instanzobjekt des Dateiüberwachers, das für die Überwachung
            des Konfigurationsverzeichnisses verantwortlich ist.
        """

        class ConfigHandler(FileSystemEventHandler):
            def __init__(self, scheduler: "JobScheduler") -> None:
                self.scheduler = scheduler
            
            def on_modified(self, event: FileSystemEventHandler) -> None:
                if not event.is_directory and event.src_path.endswith('.toml'):
                    self.scheduler._handle_config_change(Path(event.src_path))

        self.observer = Observer()
        self.observer.schedule(
            ConfigHandler(self),
            path=str(self.config_dir),
            recursive=False
        )
        self.observer.start()

    def _handle_config_change(self, config_path: Path) -> None:
        self.logger.info(f"Config file changed: {config_path.name}")
        try:
            with open(config_path) as f:
                new_config = toml.load(f)
            
            job_name: str = new_config['metadata']['name']
            
            if job_name in self.jobs:
                self._update_job(job_name, new_config)
            else:
                self.add_job(config_path)
                
        except Exception as e:
            self.logger.error(f"Error reloading config {config_path}: {str(e)}")

    def _load_global_config(self) -> None:
        """
        Lädt die globale Konfiguration aus der Datei "global.toml" im Konfigurationsverzeichnis.
        Wenn die Datei existiert, wird sie geladen und der Standard-Virtual-Environment-Pfad
        (default_venv) wird gesetzt. Ein Protokolleintrag wird erstellt, um den Erfolg des Ladevorgangs
        zu bestätigen.
        Rückgabewert:
            None
        """
        global_config: Path = self.config_dir / "global.toml"
        if global_config.exists():
            with open(global_config) as f:
                config = toml.load(f)
                self.default_venv = config.get('global', {}).get('default_venv')
                self.logger.info(f"Loaded global config: {config}")

    def load_jobs(self) -> None:
        self.logger.info("Loading jobs from config directory...")
        for config_file in self.config_dir.glob("*.toml"):
            if config_file.name == "global.toml":
                continue
                
            try:
                with open(config_file) as f:
                    config = toml.load(f)
                
                if config['metadata'].get('enabled', True):
                    self._schedule_job(config)
                    self.logger.info(f"Successfully loaded job: {config['metadata']['name']}")
                else:
                    self.logger.info(f"Skipping disabled job: {config['metadata']['name']}")
                    
            except Exception as e:
                self.logger.error(f"Error loading job from {config_file}: {str(e)}")

    def add_job(self, config_path: Path) -> bool:
        try:
            with open(config_path) as f:
                config = toml.load(f)
            
            if config['metadata'].get('enabled', True):
                job_name: str = config['metadata']['name']
                if job_name in self.jobs:
                    self.logger.warning(f"Job {job_name} already exists, updating instead")
                    self._update_job(job_name, config)
                else:
                    self._schedule_job(config)
                    self.logger.info(f"Added new job: {job_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error adding job: {str(e)}")
            return False

    def _update_job(self, job_name: str, new_config: Dict[str, Any]) -> None:
        schedule.clear(job_name)
        self._schedule_job(new_config)
        self.logger.info(f"Updated job: {job_name}")

    def _schedule_job(self, config: Dict[str, Any]) -> None:
        exec_cfg: Dict[str, Any] = config['execution']
        job_name: str = config['metadata']['name']
        job_func = self._create_job_func(exec_cfg, job_name)
        frequency: str = exec_cfg['frequency'].lower()
        
        self.logger.info(f"Scheduling job: {job_name}")
        self.logger.info(f"  Script: {exec_cfg['script']}")
        self.logger.info(f"  Config: {exec_cfg.get('config', 'None')}")
        self.logger.info(f"  VENV: {exec_cfg.get('venv', self.default_venv or 'System Python')}")
        self.logger.info(f"  Frequency: {frequency}")

        if frequency == "multi_daily":
            for time_str in exec_cfg['times']:
                schedule.every().day.at(time_str).do(job_func).tag(job_name)
                next_run = self._calculate_next_run(time_str)
                self.logger.info(f"  Scheduled at {time_str} (next: {next_run.strftime('%Y-%m-%d %H:%M')})")
        
        elif frequency == "daily":
            time_str: Optional[str] = exec_cfg.get('time')
            if time_str:
                schedule.every().day.at(time_str).do(job_func).tag(job_name)
                next_run = self._calculate_next_run(time_str)
                self.logger.info(f"  Scheduled daily at {time_str} (next: {next_run.strftime('%Y-%m-%d %H:%M')})")
        
        elif frequency == "weekly":
            time_str: Optional[str] = exec_cfg.get('time')
            day: str = exec_cfg.get('day', 'monday').lower()
            getattr(schedule.every(), day).at(time_str).do(job_func).tag(job_name)
            next_run = self._calculate_next_run(time_str, day)
            self.logger.info(f"  Scheduled weekly on {day} at {time_str} (next: {next_run.strftime('%Y-%m-%d %H:%M')})")
        
        elif frequency == "monthly":
            day: int = int(exec_cfg.get('day', 1))
            time_str: str = exec_cfg.get('time', '00:00')
            schedule.every().month.on(day).at(time_str).do(job_func).tag(job_name)
            next_run = self._calculate_next_monthly_run(day, time_str)
            self.logger.info(f"  Scheduled monthly on day {day} at {time_str} (next: {next_run.strftime('%Y-%m-%d %H:%M')})")
        
        elif frequency.startswith("every_"):
            unit: str
            value: int
            unit, value = frequency.split('_')[1], int(frequency.split('_')[2])
            getattr(schedule.every(value), unit).do(job_func).tag(job_name)
            self.logger.info(f"  Scheduled every {value} {unit}")

        self.jobs[job_name] = {
            'config': config,
            'last_run': None,
            'next_run': schedule.next_run(job_func)
        }

    def _calculate_next_run(self, time_str: str, day: Optional[str] = None) -> datetime:
        now: datetime = datetime.now()
        hour, minute = map(int, time_str.split(':'))
        
        if day:
            next_run: datetime = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            while next_run.weekday() != ['monday','tuesday','wednesday','thursday','friday','saturday','sunday'].index(day):
                next_run += timedelta(days=1)
        else:
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        if next_run < now:
            if day:
                next_run += timedelta(weeks=1)
            else:
                next_run += timedelta(days=1)
        
        return next_run

    def _calculate_next_monthly_run(self, day: int, time_str: str) -> datetime:
        now: datetime = datetime.now()
        hour, minute = map(int, time_str.split(':'))
        
        # Find next occurrence of the specified day in month
        next_run: datetime = now.replace(day=day, hour=hour, minute=minute, second=0, microsecond=0)
        if next_run < now:
            try:
                next_run = (now.replace(month=now.month+1, day=1) if now.month < 12 
                          else now.replace(year=now.year+1, month=1, day=1)).replace(day=day)
            except ValueError:  # Day out of range for month
                next_run = (now.replace(month=now.month+1, day=1) if now.month < 12 else now.replace(year=now.year+1, month=1, day=1))
                while True:
                    try:
                        next_run = next_run.replace(day=day)
                        break
                    except ValueError:
                        next_run += timedelta(days=1)
        
        return next_run

    def _create_job_func(self, exec_cfg: Dict[str, Any], job_name: str) -> Any:
        def wrapped_job() -> None:
            start_time: datetime = datetime.now()
            self.logger.info(f"STARTING JOB: {job_name} at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.jobs[job_name]['last_run'] = start_time
            self.jobs[job_name]['next_run'] = schedule.next_run(wrapped_job)
            
            try:
                self._run_script(
                    exec_cfg['script'],
                    exec_cfg.get('config'),
                    exec_cfg.get('venv', self.default_venv)
                )
            except Exception as e:
                self.logger.error(f"JOB FAILED: {job_name} - {str(e)}")
            
            duration: timedelta = datetime.now() - start_time
            self.logger.info(f"COMPLETED JOB: {job_name} in {duration.total_seconds():.2f}s")
        
        return wrapped_job

    def _run_script(self, script_path: str, config_path: Optional[str], venv_path: Optional[str] = None) -> None:
        script_path = Path(script_path)
        config_path = Path(config_path) if config_path else None
        
        if venv_path:
            venv_path = Path(venv_path)
            if os.name == 'nt':
                python_exec = venv_path / "Scripts" / "python.exe"
                activate_cmd = f"call {venv_path / 'Scripts' / 'activate.bat'} && "
            else:
                python_exec = venv_path / "bin" / "python"
                activate_cmd = f"source {venv_path / 'bin' / 'activate'} && "
            
            if not python_exec.exists():
                self.logger.error(f"VENV not found at {venv_path}")
                raise FileNotFoundError(f"VENV not found at {venv_path}")
        else:
            python_exec = "python"
            activate_cmd = ""

        base_cmd = f"{python_exec} {script_path}"
        if config_path:
            base_cmd += f" -c {config_path}"
        
        full_cmd = f"{activate_cmd}{base_cmd}" if activate_cmd else base_cmd
        
        self.logger.debug(f"Executing command: {full_cmd}")
        self.logger.info(f"Arbeitsverzeichnis: {script_path.parent}")
        try:
            result = subprocess.run(
                full_cmd,
                shell=bool(activate_cmd),
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=3600,  # 1 hour timeout
                cwd=script_path.parent  # Arbeitsverzeichnis auf Script-Ordner setzen
            )
            if result.stdout:
                self.logger.info(f"Job output:\n{result.stdout.strip()}")
        except subprocess.TimeoutExpired:
            self.logger.error("Job timed out after 1 hour")
            raise
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Job failed with code {e.returncode}:\n{e.stderr.strip()}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            raise

    def run_pending(self) -> None:
        self.logger.info("Scheduler started, waiting for jobs...")
        try:
            while True:
                next_run: Optional[float] = schedule.idle_seconds()
                if next_run is None:
                    next_run = 1
                
                self._log_upcoming_jobs()
                time.sleep(min(next_run, 60))  # Max 60s sleep for responsiveness
                schedule.run_pending()
        except KeyboardInterrupt:
            self.logger.info("Shutting down scheduler...")
        finally:
            self.observer.stop()
            self.observer.join()

    def _log_upcoming_jobs(self) -> None:
        now: datetime = datetime.now()
        upcoming: list[tuple[str, str, str]] = []
        
        for job_name, job_data in self.jobs.items():
            if job_data['next_run']:
                time_remaining: float = (job_data['next_run'] - now).total_seconds()
                if 0 < time_remaining < 86400:  # Nur Jobs in den nächsten 24h
                    upcoming.append((
                        job_data['next_run'].strftime('%Y-%m-%d %H:%M'),
                        job_name,
                        self._format_timedelta(time_remaining)
                    ))
        
        if upcoming:
            self.logger.info("Upcoming jobs in next 24 hours:")
            for time_str, name, remaining in sorted(upcoming):
                self.logger.info(f"  {time_str} - {name} ({remaining})")

    def _format_timedelta(self, seconds: float) -> str:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours)}h {int(minutes)}m"
    def __init__(self, config_dir="config"):
        self.config_dir = Path(config_dir)
        self.jobs = {}
        self.default_venv = None
        self.logger = self._setup_logging()
        self._init_file_watcher()
        self._load_global_config()
        
        self.logger.info("="*50)
        self.logger.info("Initializing Job Scheduler")
        self.logger.info(f"Config directory: {self.config_dir.absolute()}")
        self.logger.info(f"Default VENV: {self.default_venv or 'Not set'}")
        self.logger.info("="*50)

    def _setup_logging(self):
        Path("logs").mkdir(exist_ok=True)
        handler = RotatingFileHandler(
            'logs/scheduler.log',
            maxBytes=5*1024*1024,  # 5 MB
            backupCount=5,
            encoding='utf-8'
        )
        logging.basicConfig(
            handlers=[handler],
            level=logging.INFO,
            format='%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Auch auf der Konsole ausgeben
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s'))
        logging.getLogger().addHandler(console_handler)
        logging.getLogger().setLevel(logging.INFO)


        return logging.getLogger()

    def _init_file_watcher(self):
        class ConfigHandler(FileSystemEventHandler):
            def __init__(self, scheduler):
                self.scheduler = scheduler
            
            def on_modified(self, event):
                if not event.is_directory and event.src_path.endswith('.toml'):
                    self.scheduler._handle_config_change(Path(event.src_path))

        self.observer = Observer()
        self.observer.schedule(
            ConfigHandler(self),
            path=str(self.config_dir),
            recursive=False
        )
        self.observer.start()

    def _handle_config_change(self, config_path):
        self.logger.info(f"Config file changed: {config_path.name}")
        try:
            with open(config_path) as f:
                new_config = toml.load(f)
            
            job_name = new_config['metadata']['name']
            
            if job_name in self.jobs:
                self._update_job(job_name, new_config)
            else:
                self.add_job(config_path)
                
        except Exception as e:
            self.logger.error(f"Error reloading config {config_path}: {str(e)}")

    def _load_global_config(self):
        global_config = self.config_dir / "global.toml"
        if global_config.exists():
            with open(global_config) as f:
                config = toml.load(f)
                self.default_venv = config.get('global', {}).get('default_venv')
                self.logger.info(f"Loaded global config: {config}")

    def load_jobs(self):
        self.logger.info("Loading jobs from config directory...")
        for config_file in self.config_dir.glob("*.toml"):
            if config_file.name == "global.toml":
                continue
                
            try:
                with open(config_file) as f:
                    config = toml.load(f)
                
                if config['metadata'].get('enabled', True):
                    self._schedule_job(config)
                    self.logger.info(f"Successfully loaded job: {config['metadata']['name']}\n")
                else:
                    self.logger.info(f"Skipping disabled job: {config['metadata']['name']}")
                    
            except Exception as e:
                self.logger.error(f"Error loading job from {config_file}: {str(e)}")

    def add_job(self, config_path):
        try:
            with open(config_path) as f:
                config = toml.load(f)
            
            if config['metadata'].get('enabled', True):
                job_name = config['metadata']['name']
                if job_name in self.jobs:
                    self.logger.warning(f"Job {job_name} already exists, updating instead")
                    self._update_job(job_name, config)
                else:
                    self._schedule_job(config)
                    self.logger.info(f"Added new job: {job_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error adding job: {str(e)}")
            return False

    def _update_job(self, job_name, new_config):
        schedule.clear(job_name)
        self._schedule_job(new_config)
        self.logger.info(f"Updated job: {job_name}\n")

    def _schedule_job(self, config):
        exec_cfg = config['execution']
        job_name = config['metadata']['name']
        job_func = self._create_job_func(exec_cfg, job_name)
        frequency = exec_cfg['frequency'].lower()
        
        self.logger.info(f"Scheduling job: {job_name}")
        self.logger.info(f"  Script: {exec_cfg['script']}")
        self.logger.info(f"  Config: {exec_cfg.get('config', 'None')}")
        self.logger.info(f"  VENV: {exec_cfg.get('venv', self.default_venv or 'System Python')}")
        self.logger.info(f"  Frequency: {frequency}")

        if frequency == "multi_daily":
            for time_str in exec_cfg['times']:
                schedule.every().day.at(time_str).do(job_func).tag(job_name)
                next_run = self._calculate_next_run(time_str)
                self.logger.info(f"  Scheduled at {time_str} (next: {next_run.strftime('%Y-%m-%d %H:%M')})")
        
        elif frequency == "daily":
            time_str = exec_cfg.get('time')
            if time_str:
                schedule.every().day.at(time_str).do(job_func).tag(job_name)
                next_run = self._calculate_next_run(time_str)
                self.logger.info(f"  Scheduled daily at {time_str} (next: {next_run.strftime('%Y-%m-%d %H:%M')})")
        
        elif frequency == "weekly":
            time_str = exec_cfg.get('time')
            day = exec_cfg.get('day', 'monday').lower()
            getattr(schedule.every(), day).at(time_str).do(job_func).tag(job_name)
            next_run = self._calculate_next_run(time_str, day)
            self.logger.info(f"  Scheduled weekly on {day} at {time_str} (next: {next_run.strftime('%Y-%m-%d %H:%M')})")
        
        elif frequency == "monthly":
            day = int(exec_cfg.get('day', 1))
            time_str = exec_cfg.get('time', '00:00')
            schedule.every().month.on(day).at(time_str).do(job_func).tag(job_name)
            next_run = self._calculate_next_monthly_run(day, time_str)
            self.logger.info(f"  Scheduled monthly on day {day} at {time_str} (next: {next_run.strftime('%Y-%m-%d %H:%M')})")
        
        elif frequency.startswith("every_"):
            unit, value = frequency.split('_')[1], int(frequency.split('_')[2])
            getattr(schedule.every(value), unit).do(job_func).tag(job_name)
            self.logger.info(f"  Scheduled every {value} {unit}")

        self.jobs[job_name] = {
            'config': config,
            'last_run': None,
            'next_run': schedule.next_run(job_func)
        }

    def _calculate_next_run(self, time_str, day=None):
        now = datetime.now()
        hour, minute = map(int, time_str.split(':'))
        
        if day:
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            while next_run.weekday() != ['monday','tuesday','wednesday','thursday','friday','saturday','sunday'].index(day):
                next_run += timedelta(days=1)
        else:
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        if next_run < now:
            if day:
                next_run += timedelta(weeks=1)
            else:
                next_run += timedelta(days=1)
        
        return next_run

    def _calculate_next_monthly_run(self, day, time_str):
        now = datetime.now()
        hour, minute = map(int, time_str.split(':'))
        
        # Find next occurrence of the specified day in month
        next_run = now.replace(day=day, hour=hour, minute=minute, second=0, microsecond=0)
        if next_run < now:
            try:
                next_run = (now.replace(month=now.month+1, day=1) if now.month < 12 
                          else now.replace(year=now.year+1, month=1, day=1)).replace(day=day)
            except ValueError:  # Day out of range for month
                next_run = (now.replace(month=now.month+1, day=1) if now.month < 12 else now.replace(year=now.year+1, month=1, day=1))
                while True:
                    try:
                        next_run = next_run.replace(day=day)
                        break
                    except ValueError:
                        next_run += timedelta(days=1)
        
        return next_run

    def _create_job_func(self, exec_cfg, job_name):
        def wrapped_job():
            start_time = datetime.now()
            self.logger.info(f"STARTING JOB: {job_name} at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.jobs[job_name]['last_run'] = start_time
            self.jobs[job_name]['next_run'] = schedule.next_run(wrapped_job)
            
            try:
                self._run_script(
                    exec_cfg['script'],
                    exec_cfg.get('config'),
                    exec_cfg.get('venv', self.default_venv)
                )
            except Exception as e:
                self.logger.error(f"JOB FAILED: {job_name} - {str(e)}")
            
            duration = datetime.now() - start_time
            self.logger.info(f"COMPLETED JOB: {job_name} in {duration.total_seconds():.2f}s")
        
        return wrapped_job

    def _run_script(self, script_path, config_path, venv_path=None):
        script_path = Path(script_path)
        config_path = Path(config_path) if config_path else None
        
        if venv_path:
            venv_path = Path(venv_path)
            if os.name == 'nt':
                python_exec = venv_path / "Scripts" / "python.exe"
                activate_cmd = f"call {venv_path / 'Scripts' / 'activate.bat'} && "
            else:
                python_exec = venv_path / "bin" / "python"
                activate_cmd = f"source {venv_path / 'bin' / 'activate'} && "
            
            if not python_exec.exists():
                self.logger.error(f"VENV not found at {venv_path}")
                raise FileNotFoundError(f"VENV not found at {venv_path}")
        else:
            python_exec = "python"
            activate_cmd = ""

        base_cmd = f"{python_exec} {script_path}"
        if config_path:
            base_cmd += f" -c {config_path}"
        
        full_cmd = f"{activate_cmd}{base_cmd}" if activate_cmd else base_cmd
        
        self.logger.debug(f"Executing command: {full_cmd}")
        self.logger.info(f"Arbeitsverzeichnis: {script_path.parent}")
        try:
            result = subprocess.run(
                full_cmd,
                shell=bool(activate_cmd),
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=3600,  # 1 hour timeout
                cwd=script_path.parent  # Arbeitsverzeichnis auf Script-Ordner setzen
            )
            if result.stdout:
                self.logger.info(f"Job output:\n{result.stdout.strip()}")
        except subprocess.TimeoutExpired:
            self.logger.error("Job timed out after 1 hour")
            raise
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Job failed with code {e.returncode}:\n{e.stderr.strip()}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            raise

    def run_pending(self):
        self.logger.info("Scheduler started, waiting for jobs...")
        try:
            while True:
                next_run = schedule.idle_seconds()
                if next_run is None:
                    next_run = 1
                
                self._log_upcoming_jobs()
                if min(next_run, 60) < 1:
                    self.logger.critical("Sleeping for 1 second to avoid busy wait. Should have started earlier!")
                    time.sleep(1)
                else:
                    time.sleep(min(next_run, 60))  # Max 60s sleep for responsiveness
                schedule.run_pending()
        except KeyboardInterrupt:
            self.logger.info("Shutting down scheduler...")
        finally:
            self.observer.stop()
            self.observer.join()

    def _log_upcoming_jobs(self):
        now = datetime.now()
        upcoming = []
        
        for job_name, job_data in self.jobs.items():
            if job_data['next_run']:
                time_remaining = (job_data['next_run'] - now).total_seconds()
                if 0 < time_remaining < 86400:  # Nur Jobs in den nächsten 24h
                    upcoming.append((
                        job_data['next_run'].strftime('%Y-%m-%d %H:%M'),
                        job_name,
                        self._format_timedelta(time_remaining)
                    ))
        
        if upcoming:
            self.logger.info("Upcoming jobs in next 24 hours:")
            for time_str, name, remaining in sorted(upcoming):
                self.logger.info(f"  {time_str} - {name} ({remaining})")

    def _format_timedelta(self, seconds):
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours)}h {int(minutes)}m"

def run_scheduler():
    scheduler = JobScheduler()
    scheduler.load_jobs()
    
    tray_thread = threading.Thread(target=setup_tray_icon, daemon=True)
    tray_thread.start()
    
    scheduler.run_pending()

if __name__ == "__main__":
    run_scheduler()