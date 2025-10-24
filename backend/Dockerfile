
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
COPY static /app/static
EXPOSE 8088
CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8088"]
