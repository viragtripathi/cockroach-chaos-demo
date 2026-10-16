FROM python:3.12-slim-trixie

# docker-cli drives the nodes through the mounted socket (Docker or Podman).
# iproute2 lets this same image act as the netem helper (tc) for latency, brownouts and isolation.
# openssl makes a self-signed certificate for the changefeed scenario's webhook sink (cluster-internal only).
RUN apt-get update \
 && apt-get install -y --no-install-recommends docker-cli iproute2 openssl \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /app/tls \
 && openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=crdb-chaos-control-plane" \
      -keyout /app/tls/key.pem -out /app/tls/cert.pem 2>/dev/null

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY controlplane ./controlplane

EXPOSE 8088
CMD ["python", "-m", "controlplane"]
