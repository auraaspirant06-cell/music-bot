FROM denoland/deno:bin AS deno_bin

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Deno JS runtime required by yt-dlp for YouTube extraction challenges
COPY --from=deno_bin /deno /usr/local/bin/deno

WORKDIR /app

RUN useradd --create-home --shell /bin/bash botuser \
    && mkdir -p /app/audio_cache \
    && chown -R botuser:botuser /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=botuser:botuser music_bot.py youtube_api.py ./

USER botuser

CMD ["python", "music_bot.py"]
