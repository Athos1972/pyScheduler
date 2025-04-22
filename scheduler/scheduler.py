import schedule
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

class JobScheduler:
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
                    self.logger.info(f"Successfully loaded job: {config['metadata']['name']}")
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
        self.logger.info(f"Updated job: {job_name}")

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