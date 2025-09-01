import logging
from logging.handlers import RotatingFileHandler
import os, socket

DEFAULT_LOG_PATH = os.getenv("ETL_LOG_PATH", "logs/etl_pipeline.log")
os.makedirs(os.path.dirname(DEFAULT_LOG_PATH), exist_ok=True)

def setup_logging(name: str = "etl", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)

    fmt = logging.Formatter(
        fmt='%(asctime)s | %(levelname)s | %(name)s | %(message)s | host=%(host)s',
        datefmt='%Y-%m-%dT%H:%M:%S%z'
    )

    class HostFilter(logging.Filter):
        def filter(self, record):
            record.host = socket.gethostname()
            return True

    logger.addFilter(HostFilter())

    file_handler = RotatingFileHandler(DEFAULT_LOG_PATH, maxBytes=10_000_000, backupCount=5)
    file_handler.setFormatter(fmt)

    console = logging.StreamHandler()
    console.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console)
    return logger

def timed(task_name: str):
    import time, functools
    log = setup_logging()
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            log.info(f"{task_name} started")
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start
                log.info(f"{task_name} finished in {duration:.2f}s")
                return result
            except Exception as e:
                log.exception(f"{task_name} failed: {e}")
                raise
        return wrapper
    return decorator



