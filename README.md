# Telegram Group & Voice Chat Music Bot

Stream crystal-clear music straight into your Telegram groups and voice chats with lightning-fast playback, zero-gap queued transitions, smart non-stop autoplay, and built-in **YouTube Data API v3** + **VPS anti-IP-block** protection.

## Highlights

- **Direct YouTube Data API v3 Integration (`YOUTUBE_API_KEY`)**: All `/play` searches, `/search` pickers, video URL metadata, playlists, and autoplay discovery run through official YouTube Data API v3 endpoints (with multi-key quota rotation) — **zero YouTube web scraping for search/discovery on your VPS IP**.
- **Anti-VPS-IP-Block Audio Layer**:
  - **External Audio Relay / Piped / Invidious API** (`STREAM_API_URL` + `STREAM_API_KEY`) support so even audio extraction can bypass datacenter IPs.
  - **Automated PO Token Sidecar** (`bgutil-ytdlp-pot-provider` + `PO_TOKEN_SERVER`) & **Deno JS runtime** built into Docker.
  - **Automatic Player Client Rotation** (`default,tv,android_vr,web_safari`), **SOCKS5/HTTP Proxy/WARP** (`PROXY_URL`), **Cookies** (`COOKIES_FILE`), and optional **SoundCloud fallback** (`ALT_SOURCE_FALLBACK=true`).
- **Lightning-fast playback & zero interruptions**: `hybrid` streaming starts idle tracks immediately while pre-downloading queued and autoplay tracks into a bounded LRU disk cache (`audio_cache/`), with automatic fallback to local cache if a remote stream fails and auto-skip on broken tracks.
- **Ultra-rich, crystal-clear audio**: Streams audio-only pipelines via `py-tgcalls` v3 (`AudioQuality.STUDIO` 96 kHz stereo default, or `HIGH` 48 kHz stereo) with Opus-first format selection and optional FFmpeg filters (`FFMPEG_AUDIO_FILTER`).
- **Works in groups & voice chats**: Auto-joins the assistant userbot, supports Telegram Forum Topics and anonymous group admins, and exposes `/play`, `/playnext`, `/search`, `/pause`, `/resume`, `/skip`, `/stop`, `/queue`, `/np`, `/autoplay`, `/loop`, `/shuffle`, `/volume`, `/mute`, `/unmute`, `/clear`, `/status`, and `/ping` plus inline control buttons.

## Repository Structure

```text
music-bot/
├── music_bot.py          # Bot commands, PyTgCalls voice engine, cache & CLI
├── youtube_api.py        # Direct YouTube Data API v3 client & external stream resolver
├── requirements.txt      # Python dependencies
├── .env.example          # Environment variable template (API keys + anti-block config)
├── Dockerfile            # Container image with Python 3.12, FFmpeg & Deno
├── docker-compose.yml    # One-command stack with bgutil PO-token sidecar
├── music-bot.service     # systemd service unit for Ubuntu/Debian VPS
├── tests/
│   └── test_music_bot.py # Offline mocked unit & integration tests
└── README.md
```

## Quick Setup (Linux VPS — Ubuntu / Debian)

1. **Install system FFmpeg, Python venv, and Deno (for `yt-dlp` JS challenges):**
   ```bash
   sudo apt update && sudo apt install -y ffmpeg python3-venv curl unzip
   curl -fsSL https://deno.land/install.sh | sh
   ```

2. **Create virtual environment & install dependencies:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure `.env` (Telegram + YouTube Data API v3 Key):**
   ```bash
   cp .env.example .env
   nano .env
   ```
   Fill in:
   - `API_ID`, `API_HASH` (from https://my.telegram.org/apps)
   - `BOT_TOKEN` (from `@BotFather`)
   - `YOUTUBE_API_KEY` (from Google Cloud Console -> YouTube Data API v3; add multiple comma-separated keys in `YOUTUBE_API_KEYS` for automatic quota rotation)
   - Generate the assistant userbot `SESSION_STRING`:
     ```bash
     python music_bot.py --generate-session
     ```

4. **Verify dependencies & run:**
   ```bash
   python music_bot.py --check
   python music_bot.py
   ```

## Recommended: Docker Compose (Zero VPS IP Blocks)

`docker-compose.yml` launches the bot alongside the `bgutil-provider` PO-token generator so YouTube extraction works reliably on datacenter IPs:

```bash
cp .env.example .env
# Fill API_ID, API_HASH, BOT_TOKEN, SESSION_STRING, and YOUTUBE_API_KEY
docker compose up -d --build
docker compose logs -f music-bot
```

> **Tip for strict datacenter IP blocks:** Pair `YOUTUBE_API_KEY` with either:
> - **Cloudflare WARP** (`PROXY_URL=socks5://127.0.0.1:40000`) or a residential proxy, **or**
> - An external **Piped / Invidious / custom audio relay** via `STREAM_API_URL` & `STREAM_API_KEY`, **or**
> - Fresh `COOKIES_FILE=./cookies.txt` from a secondary Google account.

## Running with systemd

```bash
sudo mkdir -p /opt/music-bot
sudo cp -r . /opt/music-bot/
# Set up .venv and .env inside /opt/music-bot, then:
sudo cp music-bot.service /etc/systemd/system/music-bot.service
sudo systemctl daemon-reload
sudo systemctl enable --now music-bot
sudo journalctl -u music-bot -f
```

## Running Offline Tests

```bash
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
pip install pytest pytest-asyncio httpx python-dotenv
pytest -v
```
