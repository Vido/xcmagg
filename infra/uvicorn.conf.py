import multiprocessing

bind = "0.0.0.0:8000"

# With SQLite + WAL, 1 process is safest.
workers = 1

# Concurrency control
limit_concurrency = 1000
backlog = 2048

# Keep connections alive
timeout_keep_alive = 10

# Logging
log_level = "info"
access_log = True

# Lifespan support
lifespan = "on"

# Performance
loop = "uvloop"
http = "httptools"
