# logging_monitoring.py
import json, logging, os, time, functools, socket
from logging.handlers import RotatingFileHandler
from datetime import datetime

DEFAULT_LOG_PATH = os.getenv("ETL_LOG_PATH", "logs/etl_pipeline.log")
os.makedirs(os.path.dirname(DEFAULT_LOG_PATH), exist_ok=True)

def setup_logging(name: str = "etl", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(level)

    fmt = logging.Formatter(
        fmt='%(asctime)s | %(levelname)s | %(name)s | %(message)s | host=%(host)s',
        datefmt='%Y-%m-%dT%H:%M:%S%z'
    )
    # enrich records with host
    class HostFilter(logging.Filter):
        def filter(self, record):
            record.host = socket.gethostname()
            return True
    logger.addFilter(HostFilter())

    # File (rotating) + Console
    file_handler = RotatingFileHandler(DEFAULT_LOG_PATH, maxBytes=10_000_000, backupCount=5)
    file_handler.setFormatter(fmt)
    console = logging.StreamHandler()
    console.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console)
    return logger

# ---- Timing/metrics helpers ----

def timed(task_name: str):
    """Decorator to measure duration and auto-log start/finish/error."""
    def _wrap(func):
        @functools.wraps(func)
        def _inner(*args, **kwargs):
            log = setup_logging()
            start = time.time()
            log.info(f"{task_name} started")
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start
                log.info(f"{task_name} finished", extra={})
                emit_metric(task=task_name, metric="duration_sec", value=round(duration, 3), status="success")
                return result
            except Exception as e:
                duration = time.time() - start
                log.exception(f"{task_name} failed: {e}")
                emit_metric(task=task_name, metric="duration_sec", value=round(duration, 3), status="failed")
                raise
        return _inner
    return _wrap

def emit_metric(task: str, metric: str, value, status: str = "info", extra: dict | None = None):
    """
    Writes metrics to (priority order): Delta (if Spark available) -> CSV fallback.
    Customize storage to your environment.
    """
    payload = {
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "task": task,
        "metric": metric,
        "value": value,
        "status": status,
        **(extra or {})
    }
    

    # CSV fallback
    import csv
    csv_path = os.getenv("ETL_METRICS_CSV_PATH", "logs/etl_metrics.csv")
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(payload.keys()))
        if not exists:
            w.writeheader()
        w.writerow(payload)

def log_rowcount(df, table_name: str):
    """Record row counts per step."""
    try:
        cnt = df.count()
    except Exception:
        cnt = getattr(df, "__len__", lambda: None)() or -1
    setup_logging().info(f"rowcount {table_name} = {cnt}")
    emit_metric(task=table_name, metric="row_count", value=cnt, status="success")
    return cnt
