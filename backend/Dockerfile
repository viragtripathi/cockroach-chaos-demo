
FROM python:3.11-slim
WORKDIR /app

# Install Docker CLI
RUN apt-get update && \
    apt-get install -y curl && \
    curl -fsSL https://get.docker.com | sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py /app/app.py
COPY gunicorn.conf.py /app/gunicorn.conf.py
COPY static /app/static
EXPOSE 8088

# Use Gunicorn with Uvicorn workers for production multi-user support
# Default: 4 workers (handles ~10-20 concurrent users)
# Override with GUNICORN_WORKERS env var for larger labs
CMD ["gunicorn", "app:app", "-c", "gunicorn.conf.py"]
