#!/usr/bin/env python3
"""Telegram Group & Voice Chat High-Quality Music Streaming Bot.

Streams crystal-clear stereo audio directly into Telegram group voice chats
with lightning-fast playback, background next-track pre-fetching, smart
non-stop autoplay, and multi-layer VPS IP block protection.

Architecture:
    - Bot Client (Kurigram / Pyrogram): Handles group commands (/play, /playnext,
      /search, /pause, /resume, /skip, /autoplay, /loop, /shuffle, /queue, /np,
      /volume, /mute, /stop, /status, /ping) and inline buttons.
    - Assistant Userbot + PyTgCalls v3 (NTgCalls + FFmpeg): Joins group voice
      chats and streams 96kHz/48kHz stereo audio with zero-gap transitions.
    - YouTubeDataAPIClient (Direct API Key Integration): Resolves searches,
      video metadata, playlists, and autoplay discovery directly via official
      YouTube Data API v3 keys with automatic multi-key quota rotation.
    - MediaEngine (Disk LRU Cache + Optional External Stream API + Hardened yt-dlp):
      Delivers instant playback with hybrid streaming/pre-fetching, proxy/WARP
      isolation, bgutil PO token support, player client rotation, and fallback.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import hashlib
import html
import importlib
import ipaddress
import logging
import os
from pathlib import Path
import random
import re
import shlex
import shutil
import signal
import socket
import sys
import time
from typing import Any, Literal
from urllib.parse import urlparse

from youtube_api import (
    ExternalStreamResolver,
    YouTubeAPIQuotaError,
    YouTubeDataAPIClient,
    YouTubeVideoItem,
    clean_music_query,
    extract_youtube_playlist_id,
    extract_youtube_video_id,
    redact_secrets,
)


# ---------------------------------------------------------------------------
# Constants & Logging
# ---------------------------------------------------------------------------

AUDIO_EXTENSIONS = {
    ".aac",
    ".flac",
    ".m4a",
    ".m4b",
    ".mp3",
    ".ogg",
    ".oga",
    ".opus",
    ".wav",
    ".weba",
    ".webm",
}
STREAM_PLAYLIST_EXTENSIONS = {".m3u", ".m3u8", ".pls"}
URL_RE = re.compile(r"^https?://\S+$", re.IGNORECASE)
SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")

LOOP_CYCLE: dict[str, Literal["off", "track", "queue"]] = {
    "off": "track",
    "track": "queue",
    "queue": "off",
}

QUALITY_MAP = ("STUDIO", "HIGH", "MEDIUM", "LOW")
STREAM_MODES = ("hybrid", "cache", "direct")
DEFAULT_PLAYER_CLIENTS = ("default", "tv", "android_vr", "web_safari")
REMOTE_URL_TTL_SECONDS = 4 * 3600

logger = logging.getLogger("vc_music_bot")


def setup_logging(level_name: str = "INFO") -> None:
    """Configure structured console logging."""
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
    for noisy in ("pyrogram", "pytgcalls", "ntgcalls", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(max(level, logging.WARNING))


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_int(value: str | None, default: int, minimum: int = 0) -> int:
    if value is None or not value.strip():
        return default
    try:
        return max(minimum, int(value.strip()))
    except ValueError:
        return default


def _parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass(slots=True)
class BotConfig:
    """Runtime settings loaded from .env and process environment."""

    api_id: int
    api_hash: str
    bot_token: str
    session_string: str
    audio_quality: Literal["STUDIO", "HIGH", "MEDIUM", "LOW"] = "STUDIO"
    autoplay_default: bool = True
    prefetch_enabled: bool = True
    admin_only_controls: bool = False
    max_queue_size: int = 50
    max_track_duration_seconds: int = 7200
    max_playlist_import: int = 20
    cache_dir: Path = Path("./audio_cache")
    max_cache_files: int = 30
    search_cache_ttl_seconds: int = 900
    idle_leave_seconds: int = 180
    cookies_file: str = ""
    ffmpeg_audio_filter: str = ""
    log_level: str = "INFO"
    # Direct YouTube Data API v3 integration & anti-VPS-block controls
    youtube_api_keys: list[str] = field(default_factory=list)
    youtube_region_code: str = ""
    stream_api_url: str = ""
    stream_api_key: str = ""
    proxy_url: str = ""
    po_token_server: str = ""
    youtube_po_token: str = ""
    youtube_visitor_data: str = ""
    youtube_player_clients: tuple[str, ...] = DEFAULT_PLAYER_CLIENTS
    ytdlp_source_address: str = "0.0.0.0"
    stream_mode: Literal["hybrid", "cache", "direct"] = "hybrid"
    alt_source_fallback: bool = True

    @classmethod
    def from_env(cls, env_path: Path | None = None) -> "BotConfig":
        """Load configuration from an optional .env file and environment."""
        try:
            dotenv = importlib.import_module("dotenv")
            load_dotenv = dotenv.load_dotenv
        except ImportError:
            load_dotenv = None

        if load_dotenv is not None:
            candidate = env_path
            if candidate is None:
                local_env = Path(__file__).resolve().parent / ".env"
                cwd_env = Path.cwd() / ".env"
                candidate = local_env if local_env.exists() else cwd_env
            if candidate.exists():
                load_dotenv(candidate, override=False)

        quality_raw = os.getenv("AUDIO_QUALITY", "STUDIO").strip().upper()
        quality: Literal["STUDIO", "HIGH", "MEDIUM", "LOW"] = (
            quality_raw if quality_raw in QUALITY_MAP else "STUDIO"  # type: ignore[assignment]
        )

        stream_mode_raw = os.getenv("STREAM_MODE", "hybrid").strip().lower()
        stream_mode: Literal["hybrid", "cache", "direct"] = (
            stream_mode_raw if stream_mode_raw in STREAM_MODES else "hybrid"  # type: ignore[assignment]
        )

        merged_keys: list[str] = []
        for raw_key in (
            *_parse_csv(os.getenv("YOUTUBE_API_KEY")),
            *_parse_csv(os.getenv("YOUTUBE_API_KEYS")),
        ):
            if raw_key not in merged_keys:
                merged_keys.append(raw_key)

        clients_list = _parse_csv(os.getenv("YOUTUBE_PLAYER_CLIENTS"))
        player_clients = tuple(clients_list) if clients_list else DEFAULT_PLAYER_CLIENTS

        source_addr = os.getenv("YTDLP_SOURCE_ADDRESS", "0.0.0.0").strip()

        return cls(
            api_id=_parse_int(os.getenv("API_ID"), 0, minimum=0),
            api_hash=os.getenv("API_HASH", "").strip(),
            bot_token=os.getenv("BOT_TOKEN", "").strip(),
            session_string=os.getenv("SESSION_STRING", "").strip(),
            audio_quality=quality,
            autoplay_default=_parse_bool(os.getenv("AUTOPLAY_DEFAULT"), True),
            prefetch_enabled=_parse_bool(os.getenv("PREFETCH_ENABLED"), True),
            admin_only_controls=_parse_bool(os.getenv("ADMIN_ONLY_CONTROLS"), False),
            max_queue_size=_parse_int(os.getenv("MAX_QUEUE_SIZE"), 50, minimum=1),
            max_track_duration_seconds=_parse_int(
                os.getenv("MAX_TRACK_DURATION_SECONDS"), 7200, minimum=60
            ),
            max_playlist_import=_parse_int(os.getenv("MAX_PLAYLIST_IMPORT"), 20, minimum=1),
            cache_dir=Path(os.getenv("CACHE_DIR", "./audio_cache").strip() or "./audio_cache"),
            max_cache_files=_parse_int(os.getenv("MAX_CACHE_FILES"), 30, minimum=5),
            search_cache_ttl_seconds=_parse_int(
                os.getenv("SEARCH_CACHE_TTL_SECONDS"), 900, minimum=30
            ),
            idle_leave_seconds=_parse_int(os.getenv("IDLE_LEAVE_SECONDS"), 180, minimum=0),
            cookies_file=os.getenv("COOKIES_FILE", "").strip(),
            ffmpeg_audio_filter=os.getenv("FFMPEG_AUDIO_FILTER", "").strip(),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO",
            youtube_api_keys=merged_keys,
            youtube_region_code=os.getenv("YOUTUBE_REGION_CODE", "").strip().upper(),
            stream_api_url=os.getenv("STREAM_API_URL", "").strip(),
            stream_api_key=os.getenv("STREAM_API_KEY", "").strip(),
            proxy_url=os.getenv("PROXY_URL", "").strip(),
            po_token_server=os.getenv("PO_TOKEN_SERVER", "").strip(),
            youtube_po_token=os.getenv("YOUTUBE_PO_TOKEN", "").strip(),
            youtube_visitor_data=os.getenv("YOUTUBE_VISITOR_DATA", "").strip(),
            youtube_player_clients=player_clients,
            ytdlp_source_address=source_addr,
            stream_mode=stream_mode,
            alt_source_fallback=_parse_bool(os.getenv("ALT_SOURCE_FALLBACK"), True),
        )

    def missing_credentials(self, require_session: bool = True) -> list[str]:
        """Return names of missing required Telegram credentials."""
        missing: list[str] = []
        if self.api_id <= 0:
            missing.append("API_ID")
        if not self.api_hash:
            missing.append("API_HASH")
        if require_session and not self.bot_token:
            missing.append("BOT_TOKEN")
        if require_session and not self.session_string:
            missing.append("SESSION_STRING")
        return missing

    def secret_values(self) -> tuple[str, ...]:
        """Return configured secret strings that must never appear in logs or replies."""
        return tuple(
            s
            for s in (
                self.api_hash,
                self.bot_token,
                self.session_string,
                self.stream_api_key,
                self.proxy_url,
                *self.youtube_api_keys,
            )
            if s
        )


# ---------------------------------------------------------------------------
# Models & Formatting Helpers
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Track:
    """Represents a playable track or stream in a group queue."""

    track_id: str
    title: str
    artist: str
    duration_seconds: int
    source_url: str
    source_type: Literal["youtube", "url", "telegram", "soundcloud"]
    requested_by: str
    requester_id: int
    is_live: bool = False
    stream_target: str | None = None
    stream_headers: dict[str, str] | None = None
    stream_expires_at: float = 0.0
    thumbnail_url: str | None = None
    allow_alt_fallback: bool = False
    tg_message: Any = None


@dataclass(slots=True)
class ChatState:
    """Playback, queue, and voice-call state for a single Telegram chat."""

    chat_id: int
    autoplay: bool
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    now_playing: Track | None = None
    queue: deque[Track] = field(default_factory=deque)
    history: deque[str] = field(default_factory=lambda: deque(maxlen=40))
    prefetched_autoplay: Track | None = None
    loop_mode: Literal["off", "track", "queue"] = "off"
    volume: int = 100
    is_paused: bool = False
    is_muted: bool = False
    in_call: bool = False
    started_at: float = 0.0
    paused_at: float | None = None
    accumulated_pause: float = 0.0
    control_message_id: int | None = None
    thread_id: int | None = None
    consecutive_failures: int = 0
    prefetch_task: asyncio.Task[Any] | None = None
    idle_task: asyncio.Task[Any] | None = None

    def elapsed_seconds(self) -> int:
        """Compute current playback position in seconds."""
        if self.now_playing is None or self.started_at <= 0:
            return 0
        reference = self.paused_at if self.paused_at is not None else time.monotonic()
        elapsed = int(max(0.0, reference - self.started_at - self.accumulated_pause))
        if self.now_playing.duration_seconds > 0:
            return min(elapsed, self.now_playing.duration_seconds)
        return elapsed


def format_duration(seconds: int, is_live: bool = False) -> str:
    """Format duration seconds as MM:SS or HH:MM:SS."""
    if is_live:
        return "LIVE"
    if seconds <= 0:
        return "00:00"
    hours, rem = divmod(int(seconds), 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def format_progress_bar(elapsed: int, total: int, width: int = 14) -> str:
    """Render a compact unicode playback progress bar."""
    if total <= 0:
        return "🔴 LIVE STREAM"
    ratio = min(1.0, max(0.0, elapsed / total))
    pos = min(width - 1, int(ratio * (width - 1)))
    bar = "━" * pos + "●" + "─" * (width - 1 - pos)
    return f"{format_duration(elapsed)} {bar} {format_duration(total)}"


def build_ffmpeg_parameters(audio_filter: str) -> str | None:
    """Format an FFmpeg audio filter string for PyTgCalls v3 MediaStream."""
    flt = audio_filter.strip()
    if not flt:
        return None
    if "--audio" in flt or "--base" in flt or "---mid" in flt:
        return flt
    if flt.startswith(("-af ", "-filter:a ")):
        return f"--audio ---mid {flt}"
    return f"--audio ---mid -af {shlex.quote(flt)}"


def _safe_id(raw_id: str) -> str:
    cleaned = SAFE_FILENAME_RE.sub("_", raw_id).strip("._")
    return cleaned[:80] or hashlib.sha1(raw_id.encode("utf-8")).hexdigest()[:16]


def _is_forbidden_ip(ip_obj: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return (
        ip_obj.is_private
        or ip_obj.is_loopback
        or ip_obj.is_link_local
        or ip_obj.is_multicast
        or ip_obj.is_reserved
        or ip_obj.is_unspecified
    )


def validate_remote_url(url: str) -> str:
    """Validate that a user-supplied URL is HTTP(S) and not targeting local/private hosts."""
    parsed = urlparse(url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Only public http:// or https:// URLs are supported.")

    host = parsed.hostname.lower().strip(".")
    if host in {"localhost", "metadata.google.internal"} or host.endswith(".local"):
        raise ValueError("Local or internal network URLs are not allowed.")

    try:
        literal_ip = ipaddress.ip_address(host)
    except ValueError:
        literal_ip = None

    if literal_ip is not None:
        if _is_forbidden_ip(literal_ip):
            raise ValueError("Private or loopback IP addresses are not allowed.")
        return url.strip()

    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except OSError:
        return url.strip()

    for info in infos:
        addr_str = info[4][0]
        try:
            resolved_ip = ipaddress.ip_address(addr_str)
        except ValueError:
            continue
        if _is_forbidden_ip(resolved_ip):
            raise ValueError("Target hostname resolves to a private or loopback IP.")

    return url.strip()


def extract_youtube_id(url: str) -> str | None:
    """Backward-compatible wrapper around extract_youtube_video_id."""
    return extract_youtube_video_id(url)


def _is_bot_block_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(
        needle in msg
        for needle in (
            "sign in to confirm",
            "not a bot",
            "http error 429",
            "http error 403",
            "429: too many requests",
            "bot detection",
            "requested format is not available",
            "confirm your age",
        )
    )


# ---------------------------------------------------------------------------
# Media Resolver, Cache, and Autoplay Recommendations
# ---------------------------------------------------------------------------


class MediaEngine:
    """Handles YouTube Data API v3 search, stream relay, audio cache, and autoplay."""

    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.config.cache_dir.mkdir(parents=True, exist_ok=True)
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="media")
        self._search_cache: dict[tuple[str, int], tuple[float, list[Track]]] = {}
        self._download_locks: dict[str, asyncio.Lock] = {}
        self.yt_api = YouTubeDataAPIClient(
            config.youtube_api_keys,
            region_code=config.youtube_region_code,
        )
        self.stream_api = ExternalStreamResolver(
            config.stream_api_url,
            api_key=config.stream_api_key,
        )

    def close(self) -> None:
        """Shut down worker threads synchronously."""
        self._executor.shutdown(wait=False, cancel_futures=True)

    async def aclose(self) -> None:
        """Shut down worker threads and async HTTP clients."""
        self.close()
        await self.yt_api.close()
        await self.stream_api.close()

    def _base_ydl_opts(self, player_client: str | None = None) -> dict[str, Any]:
        opts: dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "nocheckcertificate": False,
            "noplaylist": True,
            "retries": 2,
            "fragment_retries": 3,
            "socket_timeout": 15,
        }
        if self.config.ytdlp_source_address:
            opts["source_address"] = self.config.ytdlp_source_address
        if self.config.proxy_url:
            opts["proxy"] = self.config.proxy_url

        use_cookies = bool(
            self.config.cookies_file and Path(self.config.cookies_file).is_file()
        )
        if use_cookies and player_client not in {"ios", "android", "android_vr"}:
            opts["cookiefile"] = self.config.cookies_file

        extractor_args: dict[str, dict[str, list[str]]] = {}
        yt_args: dict[str, list[str]] = {}
        if player_client and player_client != "default":
            yt_args["player_client"] = [player_client]
        if self.config.youtube_po_token:
            yt_args["po_token"] = [self.config.youtube_po_token]
        if self.config.youtube_visitor_data:
            yt_args["visitor_data"] = [self.config.youtube_visitor_data]
        if yt_args:
            extractor_args["youtube"] = yt_args

        if self.config.po_token_server:
            extractor_args["youtubepot-bgutilhttp"] = {
                "base_url": [self.config.po_token_server]
            }
        if extractor_args:
            opts["extractor_args"] = extractor_args

        return opts

    def _find_cached_file(self, track_id: str) -> Path | None:
        safe = _safe_id(track_id)
        for path in self.config.cache_dir.glob(f"{safe}.*"):
            if path.is_file() and not path.name.endswith((".part", ".ytdl", ".tmp")):
                if path.stat().st_size > 0:
                    os.utime(path, None)
                    return path
        return None

    def cached_file_count(self) -> int:
        """Return number of cached audio files on disk."""
        if not self.config.cache_dir.exists():
            return 0
        return sum(
            1
            for p in self.config.cache_dir.iterdir()
            if p.is_file() and not p.name.endswith((".part", ".ytdl", ".tmp"))
        )

    def _prune_cache(self) -> None:
        files = [
            p
            for p in self.config.cache_dir.iterdir()
            if p.is_file() and not p.name.endswith((".part", ".ytdl", ".tmp"))
        ]
        if len(files) <= self.config.max_cache_files:
            return
        files.sort(key=lambda p: p.stat().st_mtime)
        to_remove = len(files) - self.config.max_cache_files
        for old_file in files[:to_remove]:
            try:
                old_file.unlink(missing_ok=True)
            except OSError as exc:
                logger.debug("Failed to prune cache file %s: %s", old_file, exc)

    def _api_item_to_track(
        self,
        item: YouTubeVideoItem,
        requested_by: str,
        requester_id: int,
        allow_alt_fallback: bool,
    ) -> Track:
        return Track(
            track_id=item.video_id,
            title=item.title,
            artist=item.artist,
            duration_seconds=item.duration_seconds,
            source_url=item.webpage_url,
            source_type="youtube",
            requested_by=requested_by,
            requester_id=requester_id,
            is_live=item.is_live,
            thumbnail_url=item.thumbnail_url,
            allow_alt_fallback=allow_alt_fallback,
        )

    def _entry_to_track(
        self,
        entry: dict[str, Any],
        requested_by: str,
        requester_id: int,
        fallback_url: str = "",
        source_type: Literal["youtube", "url", "telegram", "soundcloud"] = "youtube",
        allow_alt_fallback: bool = False,
    ) -> Track:
        video_id = str(entry.get("id") or "").strip()
        webpage_url = str(
            entry.get("webpage_url")
            or entry.get("url")
            or fallback_url
            or (f"https://www.youtube.com/watch?v={video_id}" if video_id else "")
        )
        if webpage_url and not webpage_url.startswith(("http://", "https://")) and video_id:
            webpage_url = f"https://www.youtube.com/watch?v={video_id}"
        if not video_id:
            video_id = hashlib.sha1(webpage_url.encode("utf-8")).hexdigest()[:12]

        duration = int(entry.get("duration") or 0)
        is_live = bool(entry.get("is_live"))
        title = html.unescape(str(entry.get("title") or "Unknown Track")).strip()
        artist = html.unescape(
            str(
                entry.get("artist")
                or entry.get("uploader")
                or entry.get("channel")
                or "Unknown Artist"
            )
        ).strip()
        thumb = entry.get("thumbnail")
        return Track(
            track_id=video_id,
            title=title,
            artist=artist,
            duration_seconds=duration,
            source_url=webpage_url,
            source_type=source_type,
            requested_by=requested_by,
            requester_id=requester_id,
            is_live=is_live,
            thumbnail_url=str(thumb) if thumb else None,
            allow_alt_fallback=allow_alt_fallback,
        )

    @staticmethod
    def _clone_track(track: Track, requested_by: str, requester_id: int) -> Track:
        return Track(
            track_id=track.track_id,
            title=track.title,
            artist=track.artist,
            duration_seconds=track.duration_seconds,
            source_url=track.source_url,
            source_type=track.source_type,
            requested_by=requested_by,
            requester_id=requester_id,
            is_live=track.is_live,
            stream_target=track.stream_target,
            stream_headers=dict(track.stream_headers) if track.stream_headers else None,
            stream_expires_at=track.stream_expires_at,
            thumbnail_url=track.thumbnail_url,
            allow_alt_fallback=track.allow_alt_fallback,
        )

    def _search_sync(
        self,
        query: str,
        limit: int,
        requested_by: str,
        requester_id: int,
        allow_playlist: bool = False,
    ) -> list[Track]:
        yt_dlp = importlib.import_module("yt_dlp")
        is_url = bool(URL_RE.match(query))
        target = query if is_url else f"ytsearch{limit}:{query}"

        opts = self._base_ydl_opts()
        opts.update(
            {
                "extract_flat": "in_playlist" if not allow_playlist else True,
                "skip_download": True,
                "default_search": "ytsearch",
            }
        )
        if allow_playlist:
            opts["noplaylist"] = False
            opts["playlistend"] = limit

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(target, download=False)

        if not info:
            return []

        raw_entries = info.get("entries") if "entries" in info else [info]
        tracks: list[Track] = []
        for entry in raw_entries or []:
            if not entry:
                continue
            track = self._entry_to_track(
                entry,
                requested_by=requested_by,
                requester_id=requester_id,
                fallback_url=query if is_url else "",
                allow_alt_fallback=not is_url,
            )
            if (
                not track.is_live
                and track.duration_seconds > self.config.max_track_duration_seconds
            ):
                continue
            tracks.append(track)
            if len(tracks) >= limit:
                break
        return tracks

    async def search_tracks(
        self,
        query: str,
        requested_by: str,
        requester_id: int,
        limit: int = 1,
    ) -> list[Track]:
        """Resolve a search query, URL, or playlist into Track candidates with TTL caching."""
        clean_query = query.strip()
        if not clean_query:
            return []

        is_url = bool(URL_RE.match(clean_query))
        if is_url:
            safe_url = validate_remote_url(clean_query)
            parsed = urlparse(safe_url)
            suffix = Path(parsed.path).suffix.lower()
            if suffix in AUDIO_EXTENSIONS or suffix in STREAM_PLAYLIST_EXTENSIONS:
                track_id = hashlib.sha1(safe_url.encode("utf-8")).hexdigest()[:14]
                name = Path(parsed.path).stem or parsed.hostname or "Direct Stream"
                return [
                    Track(
                        track_id=track_id,
                        title=name,
                        artist=parsed.hostname or "Web Stream",
                        duration_seconds=0,
                        source_url=safe_url,
                        source_type="url",
                        requested_by=requested_by,
                        requester_id=requester_id,
                        is_live=suffix in STREAM_PLAYLIST_EXTENSIONS,
                        stream_target=safe_url,
                    )
                ]
            clean_query = safe_url

        cache_key = (clean_query.lower(), limit)
        now = time.monotonic()
        cached = self._search_cache.get(cache_key)
        if cached and (now - cached[0]) < self.config.search_cache_ttl_seconds:
            return [self._clone_track(t, requested_by, requester_id) for t in cached[1]]

        tracks: list[Track] = []
        is_playlist_url = is_url and "/playlist" in urlparse(clean_query).path.lower()
        yt_playlist_id = extract_youtube_playlist_id(clean_query) if is_playlist_url else None
        yt_video_id = extract_youtube_video_id(clean_query) if is_url else None

        # 1. Primary path: Direct official YouTube Data API v3 (zero VPS scraping)
        if self.yt_api.enabled:
            try:
                api_items: list[YouTubeVideoItem] = []
                if yt_playlist_id:
                    api_items = await self.yt_api.get_playlist_videos(
                        yt_playlist_id, limit=max(limit, self.config.max_playlist_import)
                    )
                elif yt_video_id:
                    api_items = await self.yt_api.get_videos([yt_video_id])
                elif not is_url:
                    api_items = await self.yt_api.search_videos(clean_query, limit=limit)

                for item in api_items:
                    if (
                        not item.is_live
                        and item.duration_seconds > self.config.max_track_duration_seconds
                    ):
                        continue
                    tracks.append(
                        self._api_item_to_track(
                            item,
                            requested_by=requested_by,
                            requester_id=requester_id,
                            allow_alt_fallback=not is_url,
                        )
                    )
            except YouTubeAPIQuotaError as exc:
                logger.warning("YouTube Data API quota fallback to yt-dlp: %s", exc)
            except Exception as exc:
                logger.warning(
                    "YouTube Data API lookup failed (%s), falling back to yt-dlp",
                    redact_secrets(str(exc), self.config.secret_values()),
                )

        # 2. Fallback path: yt-dlp (if API key not configured, non-YouTube URL, or API quota hit)
        if not tracks:
            loop = asyncio.get_running_loop()
            fetch_limit = (
                self.config.max_playlist_import if is_playlist_url else limit
            )
            tracks = await loop.run_in_executor(
                self._executor,
                self._search_sync,
                clean_query,
                fetch_limit,
                requested_by,
                requester_id,
                is_playlist_url,
            )

        if tracks:
            if len(self._search_cache) >= 500:
                self._search_cache.clear()
            self._search_cache[cache_key] = (now, tracks)
        return tracks

    def _can_stream_remote_directly(self) -> bool:
        """Return True if FFmpeg can fetch remote URLs without an IP-bound proxy."""
        return not bool(self.config.proxy_url)

    def _should_prefer_stream(self, immediate: bool) -> bool:
        if self.config.stream_mode == "direct":
            return self._can_stream_remote_directly()
        if self.config.stream_mode == "cache":
            return False
        # hybrid: instant stream on immediate playback, pre-download for queued/autoplay
        return immediate and self._can_stream_remote_directly()

    def _extract_or_download_sync(
        self, track: Track, prefer_stream: bool
    ) -> tuple[str, int, bool, str, str, dict[str, str] | None, float]:
        yt_dlp = importlib.import_module("yt_dlp")
        safe = _safe_id(track.track_id)
        existing = self._find_cached_file(track.track_id)
        if existing is not None:
            return (
                str(existing),
                track.duration_seconds,
                track.is_live,
                track.title,
                track.artist,
                None,
                0.0,
            )

        audio_fmt = "bestaudio[acodec=opus]/bestaudio[ext=m4a]/bestaudio/best"
        last_exc: Exception | None = None
        info: dict[str, Any] | None = None
        winning_client: str | None = None

        clients_to_try = list(self.config.youtube_player_clients) or ["default"]
        if track.source_type != "youtube":
            clients_to_try = ["default"]

        for client in clients_to_try:
            probe_opts = self._base_ydl_opts(player_client=client)
            probe_opts.update({"format": audio_fmt, "skip_download": True})
            try:
                with yt_dlp.YoutubeDL(probe_opts) as ydl:
                    raw_info = ydl.extract_info(track.source_url, download=False)
                if isinstance(raw_info, dict) and "entries" in raw_info and raw_info["entries"]:
                    raw_info = raw_info["entries"][0]
                if isinstance(raw_info, dict):
                    info = raw_info
                    winning_client = client
                    break
            except Exception as exc:
                last_exc = exc
                if _is_bot_block_error(exc):
                    logger.debug(
                        "YouTube client '%s' blocked for %s, trying next client",
                        client,
                        track.track_id,
                    )
                    continue
                raise

        if info is None and track.allow_alt_fallback and self.config.alt_source_fallback:
            alt_query = clean_music_query(track.artist, track.title)
            logger.warning(
                "YouTube audio extraction blocked on VPS IP for '%s'; trying SoundCloud fallback",
                alt_query,
            )
            alt_opts = self._base_ydl_opts()
            alt_opts.update({"format": audio_fmt, "skip_download": True})
            try:
                with yt_dlp.YoutubeDL(alt_opts) as ydl:
                    raw_alt = ydl.extract_info(f"scsearch1:{alt_query}", download=False)
                entries = (raw_alt or {}).get("entries") or []
                if entries and isinstance(entries[0], dict):
                    info = entries[0]
                    track.source_type = "soundcloud"
                    track.source_url = str(info.get("webpage_url") or track.source_url)
            except Exception as alt_exc:
                logger.debug("SoundCloud fallback failed: %s", alt_exc)

        if info is None:
            if last_exc is not None:
                if _is_bot_block_error(last_exc):
                    raise RuntimeError(
                        "YouTube blocked audio extraction from this VPS IP. "
                        "Configure PO_TOKEN_SERVER (bgutil-provider), PROXY_URL (WARP/residential), "
                        "STREAM_API_URL, or COOKIES_FILE in .env."
                    ) from last_exc
                raise last_exc
            raise RuntimeError(f"Failed to resolve playable audio for {track.title}")

        is_live = bool(info.get("is_live")) if isinstance(info, dict) else track.is_live
        duration = int(info.get("duration") or track.duration_seconds or 0)
        title = html.unescape(str(info.get("title") or track.title))
        artist = html.unescape(
            str(
                info.get("artist")
                or info.get("uploader")
                or info.get("channel")
                or track.artist
            )
        )
        direct_url = str(info.get("url") or "").strip()
        raw_headers = info.get("http_headers")
        stream_headers = (
            {str(k): str(v) for k, v in raw_headers.items()}
            if isinstance(raw_headers, dict)
            else None
        )
        expires_at = time.monotonic() + REMOTE_URL_TTL_SECONDS

        if (is_live or duration <= 0) and direct_url:
            return (
                direct_url,
                duration,
                bool(is_live),
                title,
                artist,
                stream_headers,
                expires_at,
            )
        if is_live or (duration > self.config.max_track_duration_seconds):
            if direct_url:
                return direct_url, duration, is_live, title, artist, stream_headers, expires_at
            raise RuntimeError("Live stream URL could not be extracted.")

        if prefer_stream and direct_url:
            return direct_url, duration, is_live, title, artist, stream_headers, expires_at

        out_tpl = str(self.config.cache_dir / f"{safe}.%(ext)s")
        dl_opts = self._base_ydl_opts(player_client=winning_client)
        dl_opts.update(
            {
                "format": audio_fmt,
                "outtmpl": out_tpl,
                "concurrent_fragment_downloads": 4,
                "overwrites": False,
            }
        )
        download_url = str(info.get("webpage_url") or track.source_url)
        try:
            with yt_dlp.YoutubeDL(dl_opts) as ydl:
                ydl.download([download_url])
            cached = self._find_cached_file(track.track_id)
            if cached is not None:
                self._prune_cache()
                return str(cached), duration, is_live, title, artist, None, 0.0
        except Exception as exc:
            logger.warning(
                "Local audio download failed (%s), falling back to direct stream URL",
                redact_secrets(str(exc), self.config.secret_values()),
            )

        if direct_url:
            return direct_url, duration, is_live, title, artist, stream_headers, expires_at
        raise RuntimeError(f"Failed to resolve playable audio for {track.title}")

    async def prepare_track(
        self,
        track: Track,
        bot_client: Any = None,
        immediate: bool = False,
        force_local_cache: bool = False,
    ) -> Track:
        """Ensure track.stream_target is populated and ready for low-latency playback."""

        def _is_ready() -> bool:
            if not track.stream_target:
                return False
            if track.source_type in {"youtube", "soundcloud"} and not track.is_live:
                is_local = not track.stream_target.startswith(("http://", "https://"))
                if force_local_cache and not is_local:
                    return False
                if is_local:
                    return Path(track.stream_target).exists()
                if track.stream_expires_at > 0 and time.monotonic() >= track.stream_expires_at:
                    return False
            return True

        if _is_ready():
            return track

        lock = self._download_locks.setdefault(track.track_id, asyncio.Lock())
        async with lock:
            if _is_ready():
                return track

            if track.source_type == "url":
                track.stream_target = track.source_url
                return track

            if track.source_type == "telegram":
                existing = self._find_cached_file(track.track_id)
                if existing is not None:
                    track.stream_target = str(existing)
                    track.tg_message = None
                    return track
                if bot_client is None or track.tg_message is None:
                    raise RuntimeError("Telegram media message is no longer available.")
                dest = str(self.config.cache_dir / f"{_safe_id(track.track_id)}.audio")
                downloaded = await bot_client.download_media(track.tg_message, file_name=dest)
                if not downloaded:
                    raise RuntimeError("Could not download Telegram media attachment.")
                track.stream_target = str(downloaded)
                track.tg_message = None
                self._prune_cache()
                return track

            # Check local disk cache first regardless of stream mode
            cached = self._find_cached_file(track.track_id)
            if cached is not None:
                track.stream_target = str(cached)
                track.stream_headers = None
                track.stream_expires_at = 0.0
                return track

            # External stream relay / Piped / Invidious API (avoids touching YouTube from VPS IP)
            if (
                track.source_type == "youtube"
                and self.stream_api.enabled
                and not force_local_cache
            ):
                try:
                    resolved = await self.stream_api.resolve_video(
                        track.track_id, track.source_url
                    )
                    if resolved is not None:
                        track.stream_target = resolved.stream_url
                        track.stream_headers = resolved.headers
                        track.stream_expires_at = time.monotonic() + REMOTE_URL_TTL_SECONDS
                        if resolved.title:
                            track.title = resolved.title
                        if resolved.artist:
                            track.artist = resolved.artist
                        if resolved.duration_seconds is not None:
                            track.duration_seconds = resolved.duration_seconds
                        if resolved.is_live is not None:
                            track.is_live = bool(resolved.is_live)
                        return track
                except Exception as exc:
                    logger.warning(
                        "External stream API failed (%s), falling back to yt-dlp",
                        redact_secrets(str(exc), self.config.secret_values()),
                    )

            prefer_stream = False if force_local_cache else self._should_prefer_stream(immediate)
            loop = asyncio.get_running_loop()
            (
                target,
                duration,
                is_live,
                title,
                artist,
                headers,
                expires_at,
            ) = await loop.run_in_executor(
                self._executor,
                self._extract_or_download_sync,
                track,
                prefer_stream,
            )
            track.stream_target = target
            track.duration_seconds = duration
            track.is_live = is_live
            track.title = title
            track.artist = artist
            track.stream_headers = headers
            track.stream_expires_at = expires_at
            return track

    def _discover_autoplay_sync(
        self, seed: Track, exclude_ids: set[str]
    ) -> Track | None:
        yt_dlp = importlib.import_module("yt_dlp")
        opts = self._base_ydl_opts()
        opts.update(
            {
                "extract_flat": True,
                "skip_download": True,
                "noplaylist": False,
                "playlistend": 15,
            }
        )

        candidates: list[dict[str, Any]] = []
        vid = extract_youtube_video_id(seed.source_url) or (
            seed.track_id if seed.source_type == "youtube" and len(seed.track_id) == 11 else None
        )
        if vid:
            mix_url = f"https://www.youtube.com/watch?v={vid}&list=RD{vid}"
            try:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(mix_url, download=False)
                candidates.extend((info or {}).get("entries") or [])
            except Exception as exc:
                logger.debug("Mix autoplay lookup failed for %s: %s", vid, exc)

        if not candidates:
            seed_query = clean_music_query(seed.artist, seed.title)
            try:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(f"ytsearch8:{seed_query} music", download=False)
                candidates.extend((info or {}).get("entries") or [])
            except Exception as exc:
                logger.debug("Fallback autoplay search failed: %s", exc)

        for entry in candidates:
            if not entry:
                continue
            track = self._entry_to_track(
                entry,
                requested_by="Autoplay",
                requester_id=0,
                allow_alt_fallback=True,
            )
            if track.track_id in exclude_ids:
                continue
            if not track.is_live and 0 < track.duration_seconds <= self.config.max_track_duration_seconds:
                return track
            if track.duration_seconds == 0 and not track.is_live:
                return track
        return None

    async def find_autoplay_track(
        self, seed: Track, exclude_ids: set[str]
    ) -> Track | None:
        """Find a related next track using YouTube Data API v3 first, with yt-dlp fallback."""
        if self.yt_api.enabled:
            try:
                item = await self.yt_api.discover_autoplay(
                    seed_title=seed.title,
                    seed_artist=seed.artist,
                    exclude_ids=exclude_ids,
                    max_duration_seconds=self.config.max_track_duration_seconds,
                )
                if item is not None:
                    return self._api_item_to_track(
                        item,
                        requested_by="Autoplay",
                        requester_id=0,
                        allow_alt_fallback=True,
                    )
            except Exception as exc:
                logger.debug(
                    "YouTube Data API autoplay lookup failed (%s), falling back",
                    redact_secrets(str(exc), self.config.secret_values()),
                )

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._discover_autoplay_sync,
            seed,
            exclude_ids,
        )


# ---------------------------------------------------------------------------
# Telegram UI Formatters
# ---------------------------------------------------------------------------


def build_now_playing_text(state: ChatState, config: BotConfig) -> str:
    """Format an HTML Now Playing status message with live progress bar."""
    track = state.now_playing
    if track is None:
        return "🔇 <b>Nothing is currently playing.</b>\nUse <code>/play &lt;song or link&gt;</code> to start streaming!"

    status_icon = "⏸ <b>Paused</b>" if state.is_paused else "🎧 <b>Now Streaming</b>"
    if state.is_muted:
        status_icon += " (🔇 Muted)"

    progress = (
        "🔴 <b>LIVE STREAM</b>"
        if track.is_live
        else f"<code>{format_progress_bar(state.elapsed_seconds(), track.duration_seconds)}</code>"
    )
    safe_title = html.escape(track.title)
    safe_artist = html.escape(track.artist)
    safe_user = html.escape(track.requested_by)

    title_line = (
        f'<a href="{html.escape(track.source_url, quote=True)}">{safe_title}</a>'
        if track.source_url.startswith(("http://", "https://"))
        else safe_title
    )
    loop_badge = {"off": "Off", "track": "🔂 Track", "queue": "🔁 Queue"}[state.loop_mode]
    auto_badge = "♾ On" if state.autoplay else "Off"

    return (
        f"{status_icon}\n\n"
        f"🎵 <b>{title_line}</b>\n"
        f"👤 {safe_artist}\n"
        f"⏱ {progress}\n\n"
        f"🙋 Requested by: <b>{safe_user}</b>\n"
        f"🔊 Volume: <b>{state.volume}%</b>  •  "
        f"🎚 Audio: <b>{config.audio_quality}</b>\n"
        f"🔁 Loop: <b>{loop_badge}</b>  •  "
        f"Autoplay: <b>{auto_badge}</b>  •  "
        f"📜 Up Next: <b>{len(state.queue)}</b>"
    )


def build_controls_keyboard(state: ChatState) -> Any:
    """Create inline playback control buttons for a chat."""
    pyrogram_types = importlib.import_module("pyrogram.types")
    InlineKeyboardButton = pyrogram_types.InlineKeyboardButton
    InlineKeyboardMarkup = pyrogram_types.InlineKeyboardMarkup

    pause_label = "▶ Resume" if state.is_paused else "⏸ Pause"
    loop_label = f"🔁 {state.loop_mode.capitalize()}"
    auto_label = f"♾ Auto: {'On' if state.autoplay else 'Off'}"

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(pause_label, callback_data="mc:pause"),
                InlineKeyboardButton("⏭ Skip", callback_data="mc:skip"),
                InlineKeyboardButton("⏹ Stop", callback_data="mc:stop"),
            ],
            [
                InlineKeyboardButton(loop_label, callback_data="mc:loop"),
                InlineKeyboardButton(auto_label, callback_data="mc:autoplay"),
                InlineKeyboardButton("🔀 Shuffle", callback_data="mc:shuffle"),
            ],
            [
                InlineKeyboardButton("📜 Queue", callback_data="mc:queue"),
                InlineKeyboardButton("🔄 Refresh", callback_data="mc:refresh"),
            ],
        ]
    )


def build_queue_text(state: ChatState) -> str:
    """Format the current track and upcoming queue list in HTML."""
    lines: list[str] = ["🎶 <b>Voice Chat Queue</b>\n"]
    if state.now_playing is not None:
        dur = format_duration(state.now_playing.duration_seconds, state.now_playing.is_live)
        lines.append(
            f"▶️ <b>Now:</b> {html.escape(state.now_playing.title)} "
            f"(<code>{dur}</code>) — {html.escape(state.now_playing.requested_by)}"
        )
    else:
        lines.append("▶️ <b>Now:</b> <i>Nothing playing</i>")

    if not state.queue:
        auto_note = " (Autoplay will pick the next track!)" if state.autoplay else ""
        lines.append(f"\n📭 <i>Queue is empty{auto_note}</i>")
        return "\n".join(lines)

    lines.append(f"\n<b>Up Next ({len(state.queue)}):</b>")
    total_secs = 0
    for idx, track in enumerate(list(state.queue)[:15], start=1):
        total_secs += track.duration_seconds
        dur = format_duration(track.duration_seconds, track.is_live)
        lines.append(
            f"{idx}. {html.escape(track.title)} "
            f"(<code>{dur}</code>) — {html.escape(track.requested_by)}"
        )
    if len(state.queue) > 15:
        lines.append(f"<i>...and {len(state.queue) - 15} more</i>")
    lines.append(
        f"\n⏱ Total queued duration: <code>{format_duration(total_secs)}</code>"
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Core Voice Chat & Bot Service
# ---------------------------------------------------------------------------


class MusicBotService:
    """Coordinates Bot commands, Assistant group membership, and PyTgCalls streaming."""

    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.media = MediaEngine(config)
        self.chats: dict[int, ChatState] = {}
        self.pending_searches: dict[tuple[int, int], tuple[float, list[Track]]] = {}
        self._assistant_in_chat: set[int] = set()
        self._assistant_peer_warm: set[int] = set()
        self._assistant_id: int | None = None
        self._bot_username: str = ""
        self.bot: Any = None
        self.assistant: Any = None
        self.calls: Any = None
        self._stopped = False

    def get_chat(self, chat_id: int) -> ChatState:
        """Return or initialize ChatState for a group chat."""
        state = self.chats.get(chat_id)
        if state is None:
            state = ChatState(chat_id=chat_id, autoplay=self.config.autoplay_default)
            self.chats[chat_id] = state
        return state

    def _safe_error_text(self, exc: Exception) -> str:
        return redact_secrets(str(exc), self.config.secret_values())

    def _audio_quality(self) -> Any:
        pytgcalls_types = importlib.import_module("pytgcalls.types")
        return getattr(pytgcalls_types.AudioQuality, self.config.audio_quality)

    def _build_media_stream(self, track: Track) -> Any:
        if not track.stream_target:
            raise RuntimeError("Track has no prepared stream target.")
        pytgcalls_types = importlib.import_module("pytgcalls.types")
        MediaStream = pytgcalls_types.MediaStream
        ffmpeg_params = build_ffmpeg_parameters(self.config.ffmpeg_audio_filter)
        kwargs: dict[str, Any] = {
            "audio_parameters": self._audio_quality(),
            "video_flags": MediaStream.Flags.IGNORE,
            "ffmpeg_parameters": ffmpeg_params,
        }
        if track.stream_headers and track.stream_target.startswith(("http://", "https://")):
            kwargs["headers"] = track.stream_headers
        return MediaStream(track.stream_target, **kwargs)

    @staticmethod
    def _preview_kwargs(for_edit: bool) -> dict[str, Any]:
        pyrogram_types = importlib.import_module("pyrogram.types")
        link_preview_cls = getattr(pyrogram_types, "LinkPreviewOptions", None)
        if link_preview_cls is not None:
            return {"link_preview_options": link_preview_cls(is_disabled=True)}
        if not for_edit:
            return {"disable_web_page_preview": True}
        return {}

    async def safe_send(
        self,
        chat_id: int,
        text: str,
        reply_markup: Any = None,
        reply_to_message_id: int | None = None,
        thread_id: int | None = None,
    ) -> Any:
        """Send an HTML message while respecting Telegram FloodWait and Kurigram signatures."""
        pyrogram_enums = importlib.import_module("pyrogram.enums")
        pyrogram_errors = importlib.import_module("pyrogram.errors")
        effective_thread = thread_id
        if effective_thread is None and chat_id in self.chats:
            effective_thread = self.chats[chat_id].thread_id

        kwargs: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": pyrogram_enums.ParseMode.HTML,
            "reply_markup": reply_markup,
            "reply_to_message_id": reply_to_message_id,
            **self._preview_kwargs(for_edit=False),
        }
        if effective_thread is not None and reply_to_message_id is None:
            kwargs["message_thread_id"] = effective_thread

        for attempt in range(2):
            try:
                return await self.bot.send_message(**kwargs)
            except TypeError:
                kwargs.pop("message_thread_id", None)
                kwargs.pop("link_preview_options", None)
                return await self.bot.send_message(**kwargs)
            except pyrogram_errors.FloodWait as exc:
                if attempt == 0:
                    await asyncio.sleep(min(float(exc.value) + 0.5, 10.0))
                    continue
            except Exception as exc:
                logger.warning("Failed to send message to %s: %s", chat_id, exc)
                return None
        return None

    async def safe_edit(self, message: Any, text: str, reply_markup: Any = None) -> Any:
        """Edit an HTML message safely across Kurigram/Pyrogram versions."""
        pyrogram_enums = importlib.import_module("pyrogram.enums")
        pyrogram_errors = importlib.import_module("pyrogram.errors")
        kwargs: dict[str, Any] = {
            "text": text,
            "parse_mode": pyrogram_enums.ParseMode.HTML,
            "reply_markup": reply_markup,
            **self._preview_kwargs(for_edit=True),
        }

        async def _do_edit() -> Any:
            try:
                return await message.edit_text(**kwargs)
            except TypeError:
                fallback = dict(kwargs)
                fallback.pop("link_preview_options", None)
                return await message.edit_text(**fallback)

        try:
            return await _do_edit()
        except pyrogram_errors.FloodWait as exc:
            await asyncio.sleep(min(float(exc.value) + 0.5, 10.0))
            return await _do_edit()
        except Exception as exc:
            if "MESSAGE_NOT_MODIFIED" in str(exc):
                return message
            logger.debug("Edit message failed: %s", exc)
            return None

    async def _warm_assistant_peer(self, chat_id: int) -> None:
        """Populate the in-memory assistant peer cache before PyTgCalls resolves chat_id."""
        if chat_id in self._assistant_peer_warm or self.assistant is None:
            return
        if not hasattr(self.assistant, "get_chat"):
            self._assistant_peer_warm.add(chat_id)
            return
        try:
            await self.assistant.get_chat(chat_id)
            self._assistant_peer_warm.add(chat_id)
        except Exception as exc:
            logger.debug("Assistant peer warm-up skipped for %s: %s", chat_id, exc)

    async def ensure_assistant_in_chat(self, chat_id: int) -> None:
        """Ensure the assistant userbot has joined the target group before streaming."""
        if chat_id in self._assistant_in_chat:
            await self._warm_assistant_peer(chat_id)
            return
        if self._assistant_id is None:
            return

        pyrogram_errors = importlib.import_module("pyrogram.errors")
        pyrogram_enums = importlib.import_module("pyrogram.enums")

        try:
            member = await self.bot.get_chat_member(chat_id, self._assistant_id)
            is_restricted_non_member = (
                member.status == pyrogram_enums.ChatMemberStatus.RESTRICTED
                and getattr(member, "is_member", True) is False
            )
            if member.status not in {
                pyrogram_enums.ChatMemberStatus.LEFT,
                pyrogram_enums.ChatMemberStatus.BANNED,
            } and not is_restricted_non_member:
                self._assistant_in_chat.add(chat_id)
                await self._warm_assistant_peer(chat_id)
                return
            if member.status == pyrogram_enums.ChatMemberStatus.BANNED:
                try:
                    await self.bot.unban_chat_member(chat_id, self._assistant_id)
                except pyrogram_errors.ChatAdminRequired as exc:
                    raise RuntimeError(
                        "The assistant account is banned in this group. Unban it first."
                    ) from exc
        except pyrogram_errors.UserNotParticipant:
            pass
        except pyrogram_errors.ChatAdminRequired:
            pass

        chat = await self.bot.get_chat(chat_id)
        join_target: str | None = f"@{chat.username}" if getattr(chat, "username", None) else None

        if not join_target:
            try:
                invite = await self.bot.create_chat_invite_link(
                    chat_id, name="Music Assistant", member_limit=1
                )
                join_target = invite.invite_link
            except Exception:
                try:
                    join_target = await self.bot.export_chat_invite_link(chat_id)
                except pyrogram_errors.ChatAdminRequired as exc:
                    raise RuntimeError(
                        "Please add the music assistant account to this group or grant me "
                        "'Invite Users via Link' admin permission."
                    ) from exc

        try:
            await self.assistant.join_chat(join_target)
            self._assistant_in_chat.add(chat_id)
            self._assistant_peer_warm.add(chat_id)
        except pyrogram_errors.UserAlreadyParticipant:
            self._assistant_in_chat.add(chat_id)
            await self._warm_assistant_peer(chat_id)
        except pyrogram_errors.UserBannedInChannel as exc:
            raise RuntimeError(
                "The assistant account is banned from this group. Please unban it and retry."
            ) from exc
        except pyrogram_errors.InviteHashExpired as exc:
            raise RuntimeError("Could not join the group: invite link expired.") from exc

    def _cancel_task(self, task: asyncio.Task[Any] | None) -> None:
        if task is not None and not task.done():
            task.cancel()

    def _schedule_idle_leave(self, state: ChatState) -> None:
        self._cancel_task(state.idle_task)
        if self.config.idle_leave_seconds <= 0:
            state.idle_task = None
            return

        async def _idle_worker() -> None:
            try:
                await asyncio.sleep(self.config.idle_leave_seconds)
                async with state.lock:
                    if state.in_call and state.now_playing is None and not state.queue:
                        await self._leave_call_locked(state)
                        await self.safe_send(
                            state.chat_id,
                            "👋 Left the voice chat due to inactivity.",
                        )
            except asyncio.CancelledError:
                return

        state.idle_task = asyncio.create_task(_idle_worker())

    def _schedule_prefetch(self, state: ChatState) -> None:
        self._cancel_task(state.prefetch_task)
        if not self.config.prefetch_enabled or state.now_playing is None:
            state.prefetch_task = None
            return

        next_queued = state.queue[0] if state.queue else None
        seed = state.now_playing
        exclude_ids = set(state.history)
        exclude_ids.add(seed.track_id)
        for queued_item in state.queue:
            exclude_ids.add(queued_item.track_id)

        async def _prefetch_worker() -> None:
            try:
                if next_queued is not None:
                    await self.media.prepare_track(
                        next_queued, bot_client=self.bot, immediate=False
                    )
                    logger.debug("Pre-fetched queued track: %s", next_queued.title)
                    return

                if state.autoplay:
                    candidate = await self.media.find_autoplay_track(seed, exclude_ids)
                    if candidate is not None:
                        await self.media.prepare_track(
                            candidate, bot_client=self.bot, immediate=False
                        )
                        async with state.lock:
                            if (
                                state.now_playing is not None
                                and state.now_playing.track_id == seed.track_id
                                and state.autoplay
                                and not state.queue
                            ):
                                state.prefetched_autoplay = candidate
                                logger.debug("Pre-fetched autoplay track: %s", candidate.title)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.debug("Background pre-fetch skipped: %s", exc)

        state.prefetch_task = asyncio.create_task(_prefetch_worker())

    async def _play_prepared_stream(self, chat_id: int, track: Track) -> None:
        pytgcalls_types = importlib.import_module("pytgcalls.types")
        pytgcalls_ex = importlib.import_module("pytgcalls.exceptions")
        pyrogram_errors = importlib.import_module("pyrogram.errors")

        stream = self._build_media_stream(track)
        try:
            await self.calls.play(
                chat_id,
                stream,
                config=pytgcalls_types.GroupCallConfig(auto_start=True),
            )
        except pytgcalls_ex.NoActiveGroupCall as exc:
            raise RuntimeError(
                "No active Voice Chat found in this group. Start a Voice Chat or grant "
                "the assistant account 'Manage Video Chats' permission."
            ) from exc
        except pyrogram_errors.ChatAdminRequired as exc:
            raise RuntimeError(
                "Voice Chat is not active and the assistant lacks 'Manage Video Chats' "
                "permission to auto-start one."
            ) from exc

    async def _start_stream_locked(
        self, state: ChatState, track: Track, immediate: bool = True
    ) -> None:
        """Prepare media if needed and start/switch playback in the group's voice chat."""
        self._cancel_task(state.idle_task)
        state.idle_task = None

        await self.ensure_assistant_in_chat(state.chat_id)
        await self.media.prepare_track(track, bot_client=self.bot, immediate=immediate)

        try:
            await self._play_prepared_stream(state.chat_id, track)
        except Exception as exc:
            is_remote_yt = (
                track.source_type == "youtube"
                and not track.is_live
                and (track.stream_target or "").startswith(("http://", "https://"))
            )
            if is_remote_yt and "Voice Chat" not in str(exc):
                logger.warning(
                    "Direct stream failed for '%s' (%s); retrying via local cache",
                    track.title,
                    self._safe_error_text(exc),
                )
                track.stream_target = None
                track.stream_headers = None
                await self.media.prepare_track(
                    track,
                    bot_client=self.bot,
                    immediate=False,
                    force_local_cache=True,
                )
                await self._play_prepared_stream(state.chat_id, track)
            else:
                raise

        state.now_playing = track
        state.history.append(track.track_id)
        state.prefetched_autoplay = None
        state.in_call = True
        state.is_paused = False
        state.is_muted = False
        state.started_at = time.monotonic()
        state.paused_at = None
        state.accumulated_pause = 0.0
        state.consecutive_failures = 0

        if state.volume != 100:
            try:
                await self.calls.change_volume_call(state.chat_id, state.volume)
            except Exception:
                pass

        self._schedule_prefetch(state)

    async def _leave_call_locked(self, state: ChatState) -> None:
        self._cancel_task(state.prefetch_task)
        self._cancel_task(state.idle_task)
        state.prefetch_task = None
        state.idle_task = None
        state.now_playing = None
        state.queue.clear()
        state.prefetched_autoplay = None
        state.is_paused = False
        state.is_muted = False
        state.started_at = 0.0
        state.paused_at = None
        state.accumulated_pause = 0.0
        state.consecutive_failures = 0

        if state.in_call:
            try:
                await self.calls.leave_call(state.chat_id)
            except Exception as exc:
                logger.debug("leave_call cleanup warning for %s: %s", state.chat_id, exc)
        state.in_call = False

    def _pop_next_candidate_locked(
        self, state: ChatState, skip_requested: bool, retry_after_error: bool = False
    ) -> tuple[Track | None, Track | None]:
        """Return (immediate_next_track, previous_seed) updating queue/loop state."""
        previous = state.now_playing
        if not retry_after_error:
            if not skip_requested and state.loop_mode == "track" and previous is not None:
                return previous, previous
            if state.loop_mode == "queue" and previous is not None:
                state.queue.append(previous)
                return (state.queue.popleft() if state.queue else None), previous
        if state.queue:
            return state.queue.popleft(), previous
        if state.autoplay and previous is not None:
            if state.prefetched_autoplay is not None:
                candidate = state.prefetched_autoplay
                state.prefetched_autoplay = None
                return candidate, previous
        return None, previous

    async def _advance_locked(
        self,
        state: ChatState,
        skip_requested: bool = False,
        retry_after_error: bool = False,
    ) -> Track | None:
        """Select and start the next track (loop -> queue -> autoplay), or stop."""
        next_track, previous = self._pop_next_candidate_locked(
            state, skip_requested, retry_after_error=retry_after_error
        )

        if next_track is None and state.autoplay and previous is not None:
            exclude_ids = set(state.history)
            exclude_ids.add(previous.track_id)
            next_track = await self.media.find_autoplay_track(previous, exclude_ids)

        if next_track is None:
            self._cancel_task(state.prefetch_task)
            state.prefetch_task = None
            state.now_playing = None
            state.is_paused = False
            state.started_at = 0.0
            state.paused_at = None
            state.accumulated_pause = 0.0
            self._schedule_idle_leave(state)
            return None

        await self._start_stream_locked(state, next_track, immediate=False)
        return next_track

    async def handle_stream_ended(self, chat_id: int) -> None:
        """Advance playback seamlessly when the active audio stream finishes."""
        state = self.get_chat(chat_id)
        async with state.lock:
            if not state.in_call or state.now_playing is None:
                return

            next_track: Track | None = None
            last_error: Exception | None = None

            for attempt in range(3):
                try:
                    next_track = await self._advance_locked(
                        state,
                        skip_requested=False,
                        retry_after_error=(attempt > 0),
                    )
                    last_error = None
                    break
                except Exception as exc:
                    last_error = exc
                    state.consecutive_failures += 1
                    logger.warning(
                        "Failed to advance stream in %s (%d/3): %s",
                        chat_id,
                        state.consecutive_failures,
                        self._safe_error_text(exc),
                    )
                    if (not state.queue and not state.autoplay) or state.consecutive_failures >= 3:
                        break
                    state.prefetched_autoplay = None

            if last_error is not None:
                await self._leave_call_locked(state)
                await self.safe_send(
                    chat_id,
                    "⚠️ Playback stopped due to an error: "
                    f"<code>{html.escape(self._safe_error_text(last_error))}</code>",
                )
                return

            if next_track is not None:
                msg = await self.safe_send(
                    chat_id,
                    build_now_playing_text(state, self.config),
                    reply_markup=build_controls_keyboard(state),
                )
                if msg is not None:
                    state.control_message_id = msg.id
            else:
                await self.safe_send(
                    chat_id,
                    "✅ <b>Queue finished.</b>\n"
                    "Use <code>/play &lt;song&gt;</code> to queue more music, or "
                    "<code>/autoplay on</code> for continuous non-stop playback.",
                )

    async def handle_left_call(self, chat_id: int) -> None:
        """Reset chat state cleanly if VC closed or assistant was removed."""
        state = self.get_chat(chat_id)
        async with state.lock:
            self._cancel_task(state.prefetch_task)
            self._cancel_task(state.idle_task)
            state.prefetch_task = None
            state.idle_task = None
            state.in_call = False
            state.now_playing = None
            state.queue.clear()
            state.prefetched_autoplay = None
            state.is_paused = False
            state.is_muted = False
            state.consecutive_failures = 0
            self._assistant_in_chat.discard(chat_id)

    async def _is_admin(self, chat_id: int, user_id: int) -> bool:
        if user_id <= 0:
            return False
        pyrogram_enums = importlib.import_module("pyrogram.enums")
        try:
            member = await self.bot.get_chat_member(chat_id, user_id)
            return member.status in {
                pyrogram_enums.ChatMemberStatus.OWNER,
                pyrogram_enums.ChatMemberStatus.ADMINISTRATOR,
            }
        except Exception:
            return False

    async def check_control_permission(
        self,
        chat_id: int,
        user_id: int,
        track: Track | None,
        admin_only: bool = False,
        sender_chat_id: int | None = None,
    ) -> bool:
        """Authorize group admins, anonymous group admins, or track requester."""
        if not self.config.admin_only_controls and not admin_only:
            return True
        if sender_chat_id is not None and sender_chat_id == chat_id:
            return True
        if track is not None and not admin_only and track.requester_id == user_id:
            return True
        return await self._is_admin(chat_id, user_id)

    def _track_from_reply(self, reply: Any, requested_by: str, requester_id: int) -> Track | None:
        media = reply.audio or reply.voice or reply.video
        doc = reply.document
        if media is None and doc is not None:
            mime = (getattr(doc, "mime_type", None) or "").lower()
            fname = (getattr(doc, "file_name", None) or "").lower()
            if mime.startswith(("audio/", "video/")) or Path(fname).suffix in AUDIO_EXTENSIONS:
                media = doc

        if media is None:
            return None

        title = (
            getattr(media, "title", None)
            or getattr(media, "file_name", None)
            or ("Voice Message" if reply.voice else "Telegram Audio")
        )
        artist = getattr(media, "performer", None) or "Telegram Media"
        duration = int(getattr(media, "duration", 0) or 0)
        uid = str(getattr(media, "file_unique_id", None) or f"tg_{reply.id}")

        return Track(
            track_id=uid,
            title=str(title),
            artist=str(artist),
            duration_seconds=duration,
            source_url=getattr(reply, "link", "") or "",
            source_type="telegram",
            requested_by=requested_by,
            requester_id=requester_id,
            tg_message=reply,
        )

    async def enqueue_track(
        self,
        chat_id: int,
        track: Track,
        play_next: bool = False,
    ) -> tuple[Literal["playing", "queued"], int]:
        """Start streaming immediately if idle, otherwise insert into queue."""
        state = self.get_chat(chat_id)
        async with state.lock:
            if state.now_playing is None:
                await self._start_stream_locked(state, track, immediate=True)
                return "playing", 0

            if len(state.queue) >= self.config.max_queue_size:
                raise RuntimeError(
                    f"Queue is full (max {self.config.max_queue_size} tracks)."
                )

            if play_next:
                state.queue.appendleft(track)
                position = 1
                self._schedule_prefetch(state)
            else:
                state.queue.append(track)
                position = len(state.queue)
                if position == 1:
                    self._schedule_prefetch(state)

            return "queued", position

    async def enqueue_playlist(
        self,
        chat_id: int,
        tracks: list[Track],
        play_next: bool = False,
    ) -> tuple[bool, int]:
        """Enqueue a list of tracks, starting playback immediately if idle."""
        if not tracks:
            return False, 0
        started_immediately = False
        added_count = 0
        state = self.get_chat(chat_id)

        remaining = list(tracks)
        if state.now_playing is None:
            outcome, _ = await self.enqueue_track(chat_id, remaining[0], play_next=False)
            started_immediately = outcome == "playing"
            remaining = remaining[1:]

        ordered = list(reversed(remaining)) if play_next else remaining
        for track in ordered:
            try:
                outcome, _ = await self.enqueue_track(
                    chat_id, track, play_next=play_next
                )
                if outcome == "playing":
                    started_immediately = True
                else:
                    added_count += 1
            except RuntimeError:
                break
        return started_immediately, added_count

    async def toggle_pause_locked(
        self, state: ChatState, want_pause: bool | None = None
    ) -> Literal["paused", "resumed", "already_paused", "already_playing"]:
        """Pause or resume voice chat playback while keeping elapsed timer accurate."""
        target_pause = (not state.is_paused) if want_pause is None else want_pause
        if target_pause and not state.is_paused:
            await self.calls.pause(state.chat_id)
            state.is_paused = True
            state.paused_at = time.monotonic()
            return "paused"
        if not target_pause and state.is_paused:
            await self.calls.resume(state.chat_id)
            if state.paused_at is not None:
                state.accumulated_pause += time.monotonic() - state.paused_at
            state.paused_at = None
            state.is_paused = False
            return "resumed"
        return "already_paused" if state.is_paused else "already_playing"

    def set_autoplay_locked(
        self, state: ChatState, desired: bool | None = None
    ) -> bool:
        """Enable, disable, or toggle continuous autoplay for a chat."""
        state.autoplay = (not state.autoplay) if desired is None else desired
        if not state.autoplay:
            state.prefetched_autoplay = None
        elif state.now_playing is not None and not state.queue:
            self._schedule_prefetch(state)
        return state.autoplay

    def set_loop_locked(
        self, state: ChatState, desired: Literal["off", "track", "queue"] | None = None
    ) -> Literal["off", "track", "queue"]:
        """Set or cycle the chat repeat mode."""
        state.loop_mode = desired if desired is not None else LOOP_CYCLE[state.loop_mode]
        return state.loop_mode

    def shuffle_queue_locked(self, state: ChatState) -> bool:
        """Shuffle upcoming tracks and update next-track prefetch."""
        if len(state.queue) < 2:
            return False
        items = list(state.queue)
        random.shuffle(items)
        state.queue = deque(items)
        self._schedule_prefetch(state)
        return True

    async def _present_enqueue_result(
        self, status_msg: Any, chat_id: int, track: Track, play_next: bool = False
    ) -> None:
        """Enqueue a track and update the status message with Now Playing or Queue info."""
        outcome, pos = await self.enqueue_track(chat_id, track, play_next=play_next)
        state = self.get_chat(chat_id)
        if outcome == "playing":
            text = build_now_playing_text(state, self.config)
            kb = build_controls_keyboard(state)
            if status_msg:
                await self.safe_edit(status_msg, text, reply_markup=kb)
                async with state.lock:
                    state.control_message_id = status_msg.id
            return

        dur = format_duration(track.duration_seconds, track.is_live)
        queued_text = (
            f"➕ <b>Added to Queue (#{pos})</b>\n"
            f"🎵 {html.escape(track.title)} (<code>{dur}</code>)\n"
            f"🙋 Requested by: <b>{html.escape(track.requested_by)}</b>"
        )
        if status_msg:
            await self.safe_edit(status_msg, queued_text)

    def _build_health_summary(self, latency_ms: int | None = None) -> str:
        active_calls = sum(1 for c in self.chats.values() if c.in_call)
        yt_mode = (
            f"Direct YouTube Data API v3 ({len(self.config.youtube_api_keys)} key(s))"
            if self.media.yt_api.enabled
            else "yt-dlp fallback (no YOUTUBE_API_KEY)"
        )
        shield_bits: list[str] = []
        if self.media.stream_api.enabled:
            shield_bits.append("Stream API")
        if self.config.po_token_server:
            shield_bits.append("PO Token Server")
        if self.config.proxy_url:
            shield_bits.append("Proxy")
        if self.config.cookies_file and Path(self.config.cookies_file).is_file():
            shield_bits.append("Cookies")
        shield_str = ", ".join(shield_bits) if shield_bits else "Client Rotation + Cache"

        head = (
            f"🏓 <b>Pong!</b> <code>{latency_ms} ms</code>\n"
            if latency_ms is not None
            else "📊 <b>Bot Status &amp; Audio Engine</b>\n"
        )
        return (
            f"{head}"
            f"🎙 Active Voice Chats: <b>{active_calls}</b>\n"
            f"🎚 Audio Preset: <b>{self.config.audio_quality}</b> "
            f"(<code>{self.config.stream_mode}</code>)\n"
            f"🔑 YouTube Search: <b>{html.escape(yt_mode)}</b>\n"
            f"🛡 Anti-Block Layer: <b>{html.escape(shield_str)}</b>\n"
            f"💾 Cached Tracks: <b>{self.media.cached_file_count()}/{self.config.max_cache_files}</b>"
        )

    # -----------------------------------------------------------------------
    # Telegram Handler Registration
    # -----------------------------------------------------------------------

    def register_handlers(self) -> None:
        """Bind Kurigram bot handlers and PyTgCalls stream update handlers."""
        pyrogram = importlib.import_module("pyrogram")
        pyrogram_types = importlib.import_module("pyrogram.types")
        filters = pyrogram.filters
        InlineKeyboardButton = pyrogram_types.InlineKeyboardButton
        InlineKeyboardMarkup = pyrogram_types.InlineKeyboardMarkup

        pytgcalls_mod = importlib.import_module("pytgcalls")
        pytgcalls_types = importlib.import_module("pytgcalls.types")
        call_filters = pytgcalls_mod.filters

        # -- PyTgCalls voice events -----------------------------------------

        @self.calls.on_update(call_filters.stream_end())
        async def _on_stream_end(_, update: Any) -> None:
            StreamEnded = pytgcalls_types.StreamEnded
            if update.stream_type is None or (update.stream_type & StreamEnded.Type.AUDIO):
                await self.handle_stream_ended(update.chat_id)

        @self.calls.on_update(
            call_filters.chat_update(pytgcalls_types.ChatUpdate.Status.LEFT_CALL)
        )
        async def _on_left_call(_, update: Any) -> None:
            await self.handle_left_call(update.chat_id)

        # -- Basic info commands --------------------------------------------

        @self.bot.on_message(filters.command(["start", "help"]))
        async def _cmd_start(_, message: Any) -> None:
            username = f"@{self._bot_username}" if self._bot_username else "this bot"
            text = (
                "🎵 <b>Telegram Group &amp; Voice Chat Music Bot</b>\n\n"
                "Stream crystal-clear audio into your Telegram group voice chats "
                "with direct YouTube API integration, next-track pre-fetching, and "
                "smooth non-stop autoplay.\n\n"
                "<b>Playback Commands (in groups):</b>\n"
                "• <code>/play &lt;song, URL, or playlist&gt;</code> — Play or queue (or reply to audio/voice)\n"
                "• <code>/playnext &lt;song or URL&gt;</code> — Insert at front of queue\n"
                "• <code>/search &lt;query&gt;</code> — Pick from top 5 results\n"
                "• <code>/pause</code> / <code>/resume</code> — Pause or resume stream\n"
                "• <code>/skip</code> — Skip to next track (or autoplay next)\n"
                "• <code>/stop</code> — Clear queue and leave voice chat\n"
                "• <code>/queue</code> — Show active &amp; upcoming tracks\n"
                "• <code>/np</code> — Live progress bar &amp; audio stats\n"
                "• <code>/autoplay [on|off]</code> — Toggle non-stop autoplay\n"
                "• <code>/loop [off|track|queue]</code> — Set repeat mode\n"
                "• <code>/shuffle</code> — Shuffle queued tracks\n"
                "• <code>/volume &lt;1-200&gt;</code> — Adjust playback volume\n"
                "• <code>/mute</code> / <code>/unmute</code> — Mute or unmute voice stream\n"
                "• <code>/clear</code> — Clear queued tracks\n"
                "• <code>/status</code> / <code>/ping</code> — Check latency, API &amp; active calls\n\n"
                f"<i>Tip: Add {html.escape(username)} as a group admin with Invite &amp; "
                "Manage Voice Chats permissions, start a Voice Chat, and send /play!</i>"
            )
            await self.safe_send(
                message.chat.id, text, reply_to_message_id=message.id
            )

        @self.bot.on_message(filters.command(["ping", "status"]))
        async def _cmd_ping(_, message: Any) -> None:
            t0 = time.monotonic()
            sent = await self.safe_send(
                message.chat.id, "🏓 Checking...", reply_to_message_id=message.id
            )
            if sent is None:
                return
            latency_ms = int((time.monotonic() - t0) * 1000)
            await self.safe_edit(sent, self._build_health_summary(latency_ms=latency_ms))

        # -- Play / Playnext / Search ---------------------------------------

        @self.bot.on_message(filters.command(["play", "p", "playnext", "pn"]) & filters.group)
        async def _cmd_play(_, message: Any) -> None:
            state = self.get_chat(message.chat.id)
            if getattr(message, "message_thread_id", None) is not None:
                state.thread_id = message.message_thread_id

            parts = (message.text or "").split(maxsplit=1)
            cmd = parts[0].lstrip("/").split("@")[0].lower()
            play_next = cmd in {"playnext", "pn"}
            query = parts[1].strip() if len(parts) > 1 else ""

            user = message.from_user
            req_name = (
                user.first_name
                if user and user.first_name
                else (message.sender_chat.title if message.sender_chat else "Group Member")
            )
            req_id = user.id if user else 0

            reply_track = (
                self._track_from_reply(message.reply_to_message, req_name, req_id)
                if message.reply_to_message
                else None
            )

            if not query and reply_track is None:
                await self.safe_send(
                    message.chat.id,
                    "❓ Usage: <code>/play &lt;song name, URL, or playlist&gt;</code> "
                    "or reply to an audio/voice message.",
                    reply_to_message_id=message.id,
                )
                return

            status_msg = await self.safe_send(
                message.chat.id,
                "🔎 <b>Resolving audio...</b>",
                reply_to_message_id=message.id,
            )

            try:
                if reply_track is not None and not query:
                    await self._present_enqueue_result(
                        status_msg, message.chat.id, reply_track, play_next=play_next
                    )
                    return

                matches = await self.media.search_tracks(
                    query, requested_by=req_name, requester_id=req_id, limit=1
                )
                if not matches:
                    if status_msg:
                        await self.safe_edit(
                            status_msg,
                            f"❌ No playable results found for <code>{html.escape(query)}</code>.",
                        )
                    return

                if len(matches) == 1:
                    await self._present_enqueue_result(
                        status_msg, message.chat.id, matches[0], play_next=play_next
                    )
                    return

                started_now, queued_count = await self.enqueue_playlist(
                    message.chat.id, matches, play_next=play_next
                )
                async with state.lock:
                    np_text = build_now_playing_text(state, self.config) if started_now else ""
                    kb = build_controls_keyboard(state) if started_now else None
                summary = (
                    f"📃 <b>Imported {len(matches)} playlist tracks</b> "
                    f"(<code>{queued_count}</code> queued)."
                )
                if started_now:
                    summary = f"{summary}\n\n{np_text}"
                if status_msg:
                    await self.safe_edit(status_msg, summary, reply_markup=kb)
                    if started_now:
                        async with state.lock:
                            state.control_message_id = status_msg.id
            except Exception as exc:
                safe_err = self._safe_error_text(exc)
                logger.warning("/play failed in %s: %s", message.chat.id, safe_err)
                err_text = f"⚠️ <b>Could not play track:</b> <code>{html.escape(safe_err)}</code>"
                if status_msg:
                    await self.safe_edit(status_msg, err_text)
                else:
                    await self.safe_send(message.chat.id, err_text)

        @self.bot.on_message(filters.command(["play", "p", "playnext", "pn"]) & filters.private)
        async def _cmd_play_private(_, message: Any) -> None:
            await self.safe_send(
                message.chat.id,
                "👥 Add me to a <b>Telegram Group</b>, start a <b>Voice Chat</b>, "
                "and run <code>/play &lt;song&gt;</code> there!",
                reply_to_message_id=message.id,
            )

        @self.bot.on_message(filters.command("search") & filters.group)
        async def _cmd_search(_, message: Any) -> None:
            state = self.get_chat(message.chat.id)
            if getattr(message, "message_thread_id", None) is not None:
                state.thread_id = message.message_thread_id

            parts = (message.text or "").split(maxsplit=1)
            query = parts[1].strip() if len(parts) > 1 else ""
            if not query:
                await self.safe_send(
                    message.chat.id,
                    "❓ Usage: <code>/search &lt;song name&gt;</code>",
                    reply_to_message_id=message.id,
                )
                return

            user = message.from_user
            req_name = user.first_name if user and user.first_name else "Group Member"
            req_id = user.id if user else 0
            status_msg = await self.safe_send(
                message.chat.id,
                f"🔎 Searching for <code>{html.escape(query)}</code>...",
                reply_to_message_id=message.id,
            )
            if status_msg is None:
                return

            try:
                tracks = await self.media.search_tracks(
                    query, requested_by=req_name, requester_id=req_id, limit=5
                )
            except Exception as exc:
                await self.safe_edit(
                    status_msg,
                    f"⚠️ Search error: <code>{html.escape(self._safe_error_text(exc))}</code>",
                )
                return

            if not tracks:
                await self.safe_edit(status_msg, "❌ No matching tracks found.")
                return

            lines = [f"🔎 <b>Results for:</b> <code>{html.escape(query)}</code>\n"]
            buttons: list[Any] = []
            for idx, tr in enumerate(tracks[:5], start=1):
                dur = format_duration(tr.duration_seconds, tr.is_live)
                lines.append(
                    f"<b>{idx}.</b> {html.escape(tr.title)} — "
                    f"<i>{html.escape(tr.artist)}</i> (<code>{dur}</code>)"
                )
                buttons.append(
                    InlineKeyboardButton(str(idx), callback_data=f"mc:pick:{idx - 1}")
                )

            if len(self.pending_searches) >= 200:
                cutoff = time.monotonic() - 600
                self.pending_searches = {
                    k: v for k, v in self.pending_searches.items() if v[0] >= cutoff
                }
            self.pending_searches[(message.chat.id, status_msg.id)] = (
                time.monotonic(),
                tracks[:5],
            )
            markup = InlineKeyboardMarkup(
                [buttons, [InlineKeyboardButton("✖ Cancel", callback_data="mc:cancel_search")]]
            )
            await self.safe_edit(status_msg, "\n".join(lines), reply_markup=markup)

        # -- Playback Control Commands --------------------------------------

        @self.bot.on_message(filters.command(["pause", "resume"]) & filters.group)
        async def _cmd_pause_resume(_, message: Any) -> None:
            cmd = (message.text or "").lstrip("/").split("@")[0].split()[0].lower()
            state = self.get_chat(message.chat.id)
            user_id = message.from_user.id if message.from_user else 0
            sender_chat_id = message.sender_chat.id if message.sender_chat else None
            async with state.lock:
                if not state.in_call or state.now_playing is None:
                    await self.safe_send(
                        message.chat.id, "🔇 Nothing is playing.", reply_to_message_id=message.id
                    )
                    return
                if not await self.check_control_permission(
                    message.chat.id, user_id, state.now_playing, sender_chat_id=sender_chat_id
                ):
                    await self.safe_send(
                        message.chat.id, "⛔ Only admins or the requester can control playback."
                    )
                    return
                want_pause = cmd == "pause"
                result = await self.toggle_pause_locked(state, want_pause=want_pause)
                msg_map = {
                    "paused": "⏸ <b>Paused playback.</b>",
                    "resumed": "▶ <b>Resumed playback.</b>",
                    "already_paused": "⏸ Already paused.",
                    "already_playing": "▶ Already playing.",
                }
                await self.safe_send(message.chat.id, msg_map[result])

        @self.bot.on_message(filters.command(["skip", "next"]) & filters.group)
        async def _cmd_skip(_, message: Any) -> None:
            state = self.get_chat(message.chat.id)
            user_id = message.from_user.id if message.from_user else 0
            sender_chat_id = message.sender_chat.id if message.sender_chat else None
            async with state.lock:
                if not state.in_call or state.now_playing is None:
                    await self.safe_send(
                        message.chat.id, "🔇 Nothing to skip.", reply_to_message_id=message.id
                    )
                    return
                if not await self.check_control_permission(
                    message.chat.id, user_id, state.now_playing, sender_chat_id=sender_chat_id
                ):
                    await self.safe_send(
                        message.chat.id, "⛔ Only admins or the requester can skip."
                    )
                    return
                try:
                    next_track = await self._advance_locked(state, skip_requested=True)
                except Exception as exc:
                    await self.safe_send(
                        message.chat.id,
                        f"⚠️ Failed to skip: <code>{html.escape(self._safe_error_text(exc))}</code>",
                    )
                    return

                if next_track is not None:
                    sent = await self.safe_send(
                        message.chat.id,
                        build_now_playing_text(state, self.config),
                        reply_markup=build_controls_keyboard(state),
                    )
                    if sent:
                        state.control_message_id = sent.id
                else:
                    await self.safe_send(
                        message.chat.id,
                        "⏭ <b>Skipped.</b> Queue is now empty.",
                    )

        @self.bot.on_message(filters.command(["stop", "end", "leave"]) & filters.group)
        async def _cmd_stop(_, message: Any) -> None:
            state = self.get_chat(message.chat.id)
            user_id = message.from_user.id if message.from_user else 0
            sender_chat_id = message.sender_chat.id if message.sender_chat else None
            async with state.lock:
                if not await self.check_control_permission(
                    message.chat.id,
                    user_id,
                    state.now_playing,
                    admin_only=self.config.admin_only_controls,
                    sender_chat_id=sender_chat_id,
                ):
                    await self.safe_send(message.chat.id, "⛔ Only admins can stop playback.")
                    return
                await self._leave_call_locked(state)
            await self.safe_send(message.chat.id, "⏹ <b>Stopped playback and left the voice chat.</b>")

        @self.bot.on_message(filters.command(["queue", "q"]) & filters.group)
        async def _cmd_queue(_, message: Any) -> None:
            state = self.get_chat(message.chat.id)
            async with state.lock:
                txt = build_queue_text(state)
            await self.safe_send(message.chat.id, txt, reply_to_message_id=message.id)

        @self.bot.on_message(filters.command(["np", "nowplaying"]) & filters.group)
        async def _cmd_np(_, message: Any) -> None:
            state = self.get_chat(message.chat.id)
            async with state.lock:
                txt = build_now_playing_text(state, self.config)
                kb = build_controls_keyboard(state) if state.now_playing else None
                has_track = state.now_playing is not None
            sent = await self.safe_send(
                message.chat.id, txt, reply_markup=kb, reply_to_message_id=message.id
            )
            if sent and has_track:
                async with state.lock:
                    state.control_message_id = sent.id

        @self.bot.on_message(filters.command("autoplay") & filters.group)
        async def _cmd_autoplay(_, message: Any) -> None:
            parts = (message.text or "").split(maxsplit=1)
            arg = parts[1].strip().lower() if len(parts) > 1 else ""
            state = self.get_chat(message.chat.id)
            user_id = message.from_user.id if message.from_user else 0
            sender_chat_id = message.sender_chat.id if message.sender_chat else None
            async with state.lock:
                if not await self.check_control_permission(
                    message.chat.id, user_id, state.now_playing, sender_chat_id=sender_chat_id
                ):
                    await self.safe_send(message.chat.id, "⛔ Admin permission required.")
                    return
                desired: bool | None = None
                if arg in {"on", "true", "1"}:
                    desired = True
                elif arg in {"off", "false", "0"}:
                    desired = False
                enabled = self.set_autoplay_locked(state, desired=desired)
                status_word = "enabled ♾" if enabled else "disabled"
            await self.safe_send(message.chat.id, f"🎶 <b>Non-stop Autoplay {status_word}.</b>")

        @self.bot.on_message(filters.command("loop") & filters.group)
        async def _cmd_loop(_, message: Any) -> None:
            parts = (message.text or "").split(maxsplit=1)
            arg = parts[1].strip().lower() if len(parts) > 1 else ""
            state = self.get_chat(message.chat.id)
            user_id = message.from_user.id if message.from_user else 0
            sender_chat_id = message.sender_chat.id if message.sender_chat else None
            async with state.lock:
                if not await self.check_control_permission(
                    message.chat.id, user_id, state.now_playing, sender_chat_id=sender_chat_id
                ):
                    await self.safe_send(message.chat.id, "⛔ Admin permission required.")
                    return
                desired_loop: Literal["off", "track", "queue"] | None = (
                    arg if arg in {"off", "track", "queue"} else None  # type: ignore[assignment]
                )
                mode = self.set_loop_locked(state, desired=desired_loop)
            await self.safe_send(message.chat.id, f"🔁 <b>Loop mode set to:</b> <code>{mode}</code>")

        @self.bot.on_message(filters.command("shuffle") & filters.group)
        async def _cmd_shuffle(_, message: Any) -> None:
            state = self.get_chat(message.chat.id)
            user_id = message.from_user.id if message.from_user else 0
            sender_chat_id = message.sender_chat.id if message.sender_chat else None
            async with state.lock:
                if not await self.check_control_permission(
                    message.chat.id, user_id, state.now_playing, sender_chat_id=sender_chat_id
                ):
                    await self.safe_send(message.chat.id, "⛔ Admin permission required.")
                    return
                if not self.shuffle_queue_locked(state):
                    await self.safe_send(message.chat.id, "🔀 Need at least 2 queued tracks to shuffle.")
                    return
            await self.safe_send(message.chat.id, "🔀 <b>Shuffled the queue.</b>")

        @self.bot.on_message(filters.command("clear") & filters.group)
        async def _cmd_clear(_, message: Any) -> None:
            state = self.get_chat(message.chat.id)
            user_id = message.from_user.id if message.from_user else 0
            sender_chat_id = message.sender_chat.id if message.sender_chat else None
            async with state.lock:
                if not await self.check_control_permission(
                    message.chat.id,
                    user_id,
                    state.now_playing,
                    admin_only=self.config.admin_only_controls,
                    sender_chat_id=sender_chat_id,
                ):
                    await self.safe_send(message.chat.id, "⛔ Admin permission required.")
                    return
                count = len(state.queue)
                state.queue.clear()
                self._schedule_prefetch(state)
            await self.safe_send(message.chat.id, f"🧹 <b>Cleared {count} queued track(s).</b>")

        @self.bot.on_message(filters.command(["volume", "vol"]) & filters.group)
        async def _cmd_volume(_, message: Any) -> None:
            parts = (message.text or "").split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip().isdigit():
                await self.safe_send(message.chat.id, "❓ Usage: <code>/volume &lt;1-200&gt;</code>")
                return
            vol = int(parts[1].strip())
            if not 1 <= vol <= 200:
                await self.safe_send(message.chat.id, "⚠️ Volume must be between 1 and 200.")
                return
            state = self.get_chat(message.chat.id)
            user_id = message.from_user.id if message.from_user else 0
            sender_chat_id = message.sender_chat.id if message.sender_chat else None
            async with state.lock:
                if not state.in_call:
                    await self.safe_send(message.chat.id, "🔇 Not currently in a voice chat.")
                    return
                if not await self.check_control_permission(
                    message.chat.id,
                    user_id,
                    state.now_playing,
                    admin_only=self.config.admin_only_controls,
                    sender_chat_id=sender_chat_id,
                ):
                    await self.safe_send(message.chat.id, "⛔ Admin permission required.")
                    return
                await self.calls.change_volume_call(message.chat.id, vol)
                state.volume = vol
            await self.safe_send(message.chat.id, f"🔊 <b>Volume set to {vol}%.</b>")

        @self.bot.on_message(filters.command(["mute", "unmute"]) & filters.group)
        async def _cmd_mute(_, message: Any) -> None:
            cmd = (message.text or "").lstrip("/").split("@")[0].split()[0].lower()
            state = self.get_chat(message.chat.id)
            user_id = message.from_user.id if message.from_user else 0
            sender_chat_id = message.sender_chat.id if message.sender_chat else None
            async with state.lock:
                if not state.in_call:
                    await self.safe_send(message.chat.id, "🔇 Not currently in a voice chat.")
                    return
                if not await self.check_control_permission(
                    message.chat.id, user_id, state.now_playing, sender_chat_id=sender_chat_id
                ):
                    await self.safe_send(message.chat.id, "⛔ Admin permission required.")
                    return
                if cmd == "mute":
                    await self.calls.mute(message.chat.id)
                    state.is_muted = True
                    await self.safe_send(message.chat.id, "🔇 <b>Muted voice stream.</b>")
                else:
                    await self.calls.unmute(message.chat.id)
                    state.is_muted = False
                    await self.safe_send(message.chat.id, "🔊 <b>Unmuted voice stream.</b>")

        # -- Inline Keyboard Callbacks --------------------------------------

        @self.bot.on_callback_query(filters.regex(r"^mc:"))
        async def _on_callback(_, query: Any) -> None:
            data = query.data or ""
            chat_id = query.message.chat.id
            user_id = query.from_user.id if query.from_user else 0
            req_name = query.from_user.first_name if query.from_user else "Group Member"

            if data == "mc:cancel_search":
                self.pending_searches.pop((chat_id, query.message.id), None)
                await query.answer("Cancelled")
                await self.safe_edit(query.message, "✖ Search cancelled.")
                return

            if data.startswith("mc:pick:"):
                idx = int(data.split(":")[-1])
                entry = self.pending_searches.get((chat_id, query.message.id))
                if not entry or idx >= len(entry[1]):
                    await query.answer("Search expired. Run /search again.", show_alert=True)
                    return
                self.pending_searches.pop((chat_id, query.message.id), None)
                chosen = entry[1][idx]
                chosen.requested_by = req_name
                chosen.requester_id = user_id
                await query.answer(f"Selected: {chosen.title[:40]}")
                await self.safe_edit(query.message, "🔎 <b>Preparing track...</b>")
                try:
                    await self._present_enqueue_result(query.message, chat_id, chosen)
                except Exception as exc:
                    await self.safe_edit(
                        query.message,
                        f"⚠️ <code>{html.escape(self._safe_error_text(exc))}</code>",
                    )
                return

            state = self.get_chat(chat_id)
            async with state.lock:
                action = data.split(":", 1)[1]
                if action == "queue":
                    await query.answer(f"{len(state.queue)} queued", show_alert=False)
                    await self.safe_send(chat_id, build_queue_text(state))
                    return

                if action == "refresh":
                    await query.answer("Refreshed")
                    await self.safe_edit(
                        query.message,
                        build_now_playing_text(state, self.config),
                        reply_markup=build_controls_keyboard(state) if state.now_playing else None,
                    )
                    return

                if not await self.check_control_permission(
                    chat_id,
                    user_id,
                    state.now_playing,
                    admin_only=(action == "stop" and self.config.admin_only_controls),
                ):
                    await query.answer("Only admins or the requester can use this.", show_alert=True)
                    return

                if action == "pause":
                    if not state.in_call or state.now_playing is None:
                        await query.answer("Nothing playing.", show_alert=True)
                        return
                    result = await self.toggle_pause_locked(state)
                    await query.answer("Resumed" if result == "resumed" else "Paused")
                elif action == "skip":
                    if not state.in_call or state.now_playing is None:
                        await query.answer("Nothing to skip.", show_alert=True)
                        return
                    try:
                        next_track = await self._advance_locked(state, skip_requested=True)
                    except Exception as exc:
                        await query.answer(
                            f"Skip failed: {self._safe_error_text(exc)[:120]}", show_alert=True
                        )
                        return
                    await query.answer("Skipped" if next_track else "Queue ended")
                elif action == "stop":
                    await self._leave_call_locked(state)
                    await query.answer("Stopped")
                    await self.safe_edit(query.message, "⏹ <b>Playback stopped.</b>")
                    return
                elif action == "loop":
                    mode = self.set_loop_locked(state)
                    await query.answer(f"Loop: {mode}")
                elif action == "autoplay":
                    enabled = self.set_autoplay_locked(state)
                    await query.answer(f"Autoplay {'On' if enabled else 'Off'}")
                elif action == "shuffle":
                    if not self.shuffle_queue_locked(state):
                        await query.answer("Need 2+ queued tracks to shuffle.", show_alert=True)
                        return
                    await query.answer("Queue shuffled")

                await self.safe_edit(
                    query.message,
                    build_now_playing_text(state, self.config),
                    reply_markup=build_controls_keyboard(state) if state.now_playing else None,
                )

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    async def start(self) -> None:
        """Initialize Kurigram clients, start PyTgCalls, and idle until signal."""
        missing = self.config.missing_credentials(require_session=True)
        if missing:
            raise SystemExit(
                f"Missing required configuration in .env: {', '.join(missing)}.\n"
                "Run `python music_bot.py --generate-session` to create SESSION_STRING."
            )

        pyrogram = importlib.import_module("pyrogram")
        pytgcalls_mod = importlib.import_module("pytgcalls")

        self.bot = pyrogram.Client(
            name="vc_music_bot",
            api_id=self.config.api_id,
            api_hash=self.config.api_hash,
            bot_token=self.config.bot_token,
            in_memory=True,
        )
        self.assistant = pyrogram.Client(
            name="vc_music_assistant",
            api_id=self.config.api_id,
            api_hash=self.config.api_hash,
            session_string=self.config.session_string,
            in_memory=True,
        )
        # Instantiate PyTgCalls inside an active event loop (required on Python 3.12+)
        self.calls = pytgcalls_mod.PyTgCalls(self.assistant)
        self.register_handlers()

        await self.bot.start()
        bot_me = await self.bot.get_me()
        self._bot_username = bot_me.username or ""

        await self.calls.start()
        assistant_me = await self.assistant.get_me()
        self._assistant_id = assistant_me.id

        logger.info(
            "Bot @%s and Assistant %s (id=%s) started | AudioQuality.%s | mode=%s | yt_api_keys=%d",
            self._bot_username,
            assistant_me.first_name,
            self._assistant_id,
            self.config.audio_quality,
            self.config.stream_mode,
            len(self.config.youtube_api_keys),
        )

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        def _request_stop(*_: Any) -> None:
            if not stop_event.is_set():
                stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_stop)
            except (NotImplementedError, RuntimeError):
                signal.signal(sig, _request_stop)

        await stop_event.wait()
        await self.shutdown()

    async def shutdown(self) -> None:
        """Leave active calls, cancel background tasks, and stop Telegram clients."""
        if self._stopped:
            return
        self._stopped = True
        logger.info("Shutting down Voice Chat Music Bot...")
        for state in list(self.chats.values()):
            async with state.lock:
                await self._leave_call_locked(state)

        await self.media.aclose()
        for client in (self.assistant, self.bot):
            if client is not None:
                try:
                    if getattr(client, "is_connected", False):
                        await client.stop()
                except Exception as exc:
                    logger.debug("Error stopping client: %s", exc)
        logger.info("Shutdown complete.")


# ---------------------------------------------------------------------------
# CLI Helpers (--check, --generate-session) & Entrypoint
# ---------------------------------------------------------------------------


def run_diagnostics(config: BotConfig) -> int:
    """Verify Python version, binaries, Python packages, and anti-block config."""
    print("=== Telegram Voice Chat Music Bot Diagnostics ===")
    ok = True

    py_ok = sys.version_info >= (3, 10)
    print(f"[{'OK' if py_ok else 'FAIL'}] Python {sys.version.split()[0]} (requires >=3.10)")
    ok &= py_ok

    for binary in ("ffmpeg", "ffprobe"):
        path = shutil.which(binary)
        found = path is not None
        print(f"[{'OK' if found else 'FAIL'}] {binary}: {path or 'not found in PATH'}")
        ok &= found

    js_runtime = next((shutil.which(rt) for rt in ("deno", "node", "bun") if shutil.which(rt)), None)
    print(
        f"[{'OK' if js_runtime else 'WARN'}] JS runtime for yt-dlp: "
        f"{js_runtime or 'not found (install deno for YouTube extraction)'}"
    )

    for pkg_name, label, required in (
        ("pyrogram", "kurigram / pyrogram", True),
        ("pytgcalls", "py-tgcalls", True),
        ("ntgcalls", "ntgcalls", True),
        ("yt_dlp", "yt-dlp", True),
        ("dotenv", "python-dotenv", True),
        ("httpx", "httpx", False),
        ("yt_dlp_plugins.extractor.getpot_bgutil", "bgutil-ytdlp-pot-provider", False),
    ):
        try:
            mod = importlib.import_module(pkg_name)
            ver = getattr(mod, "__version__", "installed")
            print(f"[OK] {label}: {ver}")
        except ImportError:
            status = "FAIL" if required else "WARN"
            print(f"[{status}] {label}: not installed")
            if required:
                ok = False

    if config.youtube_api_keys:
        print(
            f"[OK] YouTube Data API v3: {len(config.youtube_api_keys)} API key(s) configured"
        )
    else:
        print(
            "[WARN] YOUTUBE_API_KEY not set — search & autoplay will use yt-dlp scraping "
            "(higher risk of VPS IP rate limits)"
        )

    if config.stream_api_url:
        print("[OK] External STREAM_API_URL configured")
    if config.po_token_server:
        print(f"[OK] PO_TOKEN_SERVER configured: {config.po_token_server}")
    if config.proxy_url:
        print("[OK] PROXY_URL configured for yt-dlp")
    if config.cookies_file:
        exists = Path(config.cookies_file).is_file()
        print(f"[{'OK' if exists else 'WARN'}] COOKIES_FILE: {config.cookies_file}")

    missing = config.missing_credentials(require_session=True)
    if missing:
        print(f"[WARN] Missing .env keys: {', '.join(missing)}")
        ok = False
    else:
        print("[OK] All required Telegram credentials present in environment/.env")

    return 0 if ok else 1


async def generate_session_string(config: BotConfig) -> None:
    """Interactively log in the assistant Telegram account and print its session string."""
    pyrogram = importlib.import_module("pyrogram")
    api_id = config.api_id or int(input("Enter API_ID: ").strip())
    api_hash = config.api_hash or input("Enter API_HASH: ").strip()

    async with pyrogram.Client(
        name=":memory:",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True,
    ) as client:
        session = await client.export_session_string()
        print("\n=== Assistant SESSION_STRING (keep private!) ===")
        print(session)
        print("================================================\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Telegram Group & Voice Chat High-Quality Music Bot"
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Optional path to .env file",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check FFmpeg, JS runtime, Python packages, and .env configuration",
    )
    parser.add_argument(
        "--generate-session",
        action="store_true",
        help="Interactively generate a Kurigram/Pyrogram SESSION_STRING for the assistant",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    args = parse_args(argv)
    config = BotConfig.from_env(args.env_file)
    setup_logging(config.log_level)

    if args.check:
        return run_diagnostics(config)

    if args.generate_session:
        asyncio.run(generate_session_string(config))
        return 0

    try:
        uvloop = importlib.import_module("uvloop")
        uvloop.install()
        logger.debug("Enabled uvloop event loop")
    except ImportError:
        pass

    service = MusicBotService(config)
    asyncio.run(service.start())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
