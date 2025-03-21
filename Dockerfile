FROM python:3.9-slim

# Instalar dependencias incluyendo cron
RUN apt-get update && apt-get install -y \
    software-properties-common \
    cron \
    unrar-free \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY crontab /etc/cron.d/app-cron
COPY entrypoint.sh .

RUN chmod 0644 /etc/cron.d/app-cron \
    && chmod +x entrypoint.sh

COPY . .

CMD ["./entrypoint.sh"]