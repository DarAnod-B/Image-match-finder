FROM python:3.10.11-slim


RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libgl1-mesa-glx \
    && sed -i 's/main/main contrib/g' /etc/apt/sources.list \
    && apt-get update && \
    echo "ttf-mscorefonts-installer msttcorefonts/accepted-mscorefonts-eula select true" | debconf-set-selections && \
    apt-get install -y ttf-mscorefonts-installer && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1


WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]