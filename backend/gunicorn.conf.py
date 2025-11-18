# Gunicorn configuration for multi-user lab environment
import os
import multiprocessing

# Worker configuration
workers = int(os.getenv("GUNICORN_WORKERS", "4"))
worker_class = "uvicorn.workers.UvicornWorker"

# Binding
bind = "0.0.0.0:8088"

# Timeouts
timeout = 120  # 2 minutes for long-running chaos operations
keepalive = 5

# Logging
accesslog = "-"  # Log to stdout
errorlog = "-"   # Log to stderr
loglevel = "info"

# Process naming
proc_name = "cockroach-chaos-demo"

# Worker lifecycle
max_requests = 1000  # Restart workers after 1000 requests (prevent memory leaks)
max_requests_jitter = 50

# Preload app for faster worker startup
preload_app = True

print(f"🚀 Starting Gunicorn with {workers} workers for multi-user support")
print(f"   Estimated capacity: {workers * 2}-{workers * 5} concurrent users")
