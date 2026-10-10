import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://dedup:dedup@localhost/dedup")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://dedup:dedup@localhost:5672/%2F")
WINDOW_MS = int(os.getenv("DEDUP_WINDOW_SECONDS", "3600")) * 1000
RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "7"))
MAX_ATTEMPTS = 5
QUEUE_PREFIX = os.getenv("QUEUE_PREFIX", "")
if WINDOW_MS <= 0 or RETENTION_DAYS <= 0:
    raise ValueError("Window and retention must be positive")
