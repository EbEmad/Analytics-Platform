FROM apache/airflow:2.8.1-python3.11
USER root
# Install system dependencies for Playwright
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libatspi2.0-0 \
    libxshmfence1 \
    libglib2.0-0 \
    libnss3-tools \
    libx11-xcb1 \
    libxcb1 \
    libpango-1.0-0 \
    libcairo2 \
    libharfbuzz0b \
    libpangocairo-1.0-0 \
    && rm -rf /var/lib/apt/lists/*
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
RUN /home/airflow/.local/bin/playwright install chromium