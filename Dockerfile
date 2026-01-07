FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
# chromium: The browser
# chromium-driver: The webdriver
# libnss3, etc: Standard libs for chrome
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
# Copy source code
COPY main.py .
COPY scraper_fechida.py .
COPY scraper_records.py .
COPY frontend/app.py .
COPY frontend/logo_rama_1.png .
COPY frontend/fondo_rama.png .
COPY pool_header_bg.png .

# Define volume location
VOLUME /app/data

# Environment variables for Chrome
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
# Python buffer
ENV PYTHONUNBUFFERED=1

# Expose Streamlit port
EXPOSE 8501

# Default command for Dashboard (can be overridden for scrapers)
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
