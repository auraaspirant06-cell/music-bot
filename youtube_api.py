"""YouTube Data API v3 client, playlist/autoplay resolver, and external stream relay support.

Designed to prevent VPS / datacenter IP blocks by routing all search, metadata,
playlist expansion, and autoplay discovery through official YouTube Data API v3 keys
(with automatic multi-key quota rotation), and optionally resolving playable audio
streams via an external relay / Piped / Invidious-compatible API.
"""

from __future__ import annotations

from dataclasses import dataclass
import html
import importlib
import json
import logging
import re
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, urlparse
from urllib.request import Request, urlopen

logger = logging.getLogger("vc_music_bot.youtube_api")

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"

ISO_DURATION_RE = re.compile(
    r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$"
)
TITLE_NOISE_RE = re.compile(
    r"\s*[\(\[](?:official\s*(?:music\s*)?video|official\s*audio|lyric\s*video|lyrics|"
    r"hd|4k|uhd|hq|mv|full\s*video|audio|video\s*song|remastered(?:\s*\d+)?)[\)\]]",
    re.IGNORECASE,
)
YOUTUBE_PLAYLIST_RE = re.compile(r"[?&]list=([A-Za-z0-9_-]+)")
YOUTUBE_VIDEO_ID_RE = re.compile(
    r"(?:youtube\.com/(?:watch\?.*v=|shorts/|live/|embed/)|youtu\.be/|music\.youtube\.com/watch\?.*v=)([A-Za-z0-9_-]{11})"
)


def parse_iso8601_duration(raw: str | None) -> int:
    """Convert an ISO-8601 YouTube duration (e.g. 'PT3M45S') into whole seconds."""
    if not raw:
        return 0
    match = ISO_DURATION_RE.match(raw.strip())
    if not match:
        return 0
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = int(match.group("seconds") or 0)
    return (((days * 24) + hours) * 60 + minutes) * 60 + seconds


def extract_youtube_video_id(url: str) -> str | None:
    """Extract an 11-character YouTube video ID from standard, Shorts, Live, or Music URLs."""
    match = YOUTUBE_VIDEO_ID_RE.search(url)
    if match:
        return match.group(1)
    parsed = urlparse(url)
    if parsed.hostname and "youtube" in parsed.hostname.lower():
        v_list = parse_qs(parsed.query).get("v")
        if v_list and re.fullmatch(r"[A-Za-z0-9_-]{11}", v_list[0]):
            return v_list[0]
    return None


def extract_youtube_playlist_id(url: str) -> str | None:
    """Extract a YouTube playlist ID from a URL if present."""
    match = YOUTUBE_PLAYLIST_RE.search(url)
    return match.group(1) if match else None


def clean_music_query(artist: str, title: str) -> str:
    """Strip noisy video tags from a track title to improve autoplay discovery."""
    cleaned_title = TITLE_NOISE_RE.sub("", title).strip(" -|")
    if not cleaned_title:
        cleaned_title = title.strip()
    clean_artist = artist.strip()
    if clean_artist and not clean_artist.lower().startswith("unknown"):
        if clean_artist.lower() in cleaned_title.lower():
            return cleaned_title
        return f"{clean_artist} {cleaned_title}"
    return cleaned_title


def redact_secrets(text: str, secrets: list[str] | tuple[str, ...]) -> str:
    """Mask API keys, proxy credentials, or tokens before logging or replying."""
    masked = text
    for secret in secrets:
        if secret and len(secret) >= 4:
            masked = masked.replace(secret, "***")
    masked = re.sub(r"(key=)[^&\s\"']+", r"\1***", masked, flags=re.IGNORECASE)
    masked = re.sub(r"(://[^:/@\s]+:)[^@/\s]+(@)", r"\1***\2", masked)
    return masked


def _pick_thumbnail(snippet: dict[str, Any]) -> str | None:
    thumbs = snippet.get("thumbnails") or {}
    for size in ("maxres", "high", "medium", "standard", "default"):
        entry = thumbs.get(size)
        if isinstance(entry, dict) and entry.get("url"):
            return str(entry["url"])
    return None


@dataclass(slots=True)
class YouTubeVideoItem:
    """Normalized metadata returned by YouTube Data API v3."""

    video_id: str
    title: str
    artist: str
    duration_seconds: int
    is_live: bool
    webpage_url: str
    thumbnail_url: str | None = None
    channel_id: str | None = None


class YouTubeAPIQuotaError(RuntimeError):
    """Raised when all configured YouTube Data API v3 keys are exhausted or invalid."""


class YouTubeDataAPIClient:
    """Async YouTube Data API v3 client with multi-key rotation and zero scraping."""

    def __init__(
        self,
        api_keys: list[str],
        region_code: str = "",
        timeout_seconds: float = 10.0,
    ) -> None:
        self.api_keys = [k.strip() for k in api_keys if k.strip()]
        self.region_code = region_code.strip().upper()
        self.timeout_seconds = timeout_seconds
        self._key_index = 0
        self._exhausted_keys: set[str] = set()
        self._http_client: Any = None

    @property
    def enabled(self) -> bool:
        """Return True if at least one YouTube Data API v3 key is configured."""
        return len(self.api_keys) > 0

    def _next_available_key(self) -> str:
        if not self.api_keys:
            raise YouTubeAPIQuotaError("No YOUTUBE_API_KEY configured.")
        total = len(self.api_keys)
        for offset in range(total):
            idx = (self._key_index + offset) % total
            candidate = self.api_keys[idx]
            if candidate not in self._exhausted_keys:
                self._key_index = (idx + 1) % total
                return candidate
        # Reset exhausted set once so transient rate limits can recover later
        self._exhausted_keys.clear()
        raise YouTubeAPIQuotaError(
            "All configured YouTube Data API v3 keys hit quota or failed."
        )

    async def close(self) -> None:
        """Close the shared async HTTP client if initialized."""
        if self._http_client is not None:
            try:
                await self._http_client.aclose()
            except Exception:
                pass
            self._http_client = None

    async def _get_http_client(self) -> Any:
        if self._http_client is not None:
            return self._http_client
        try:
            httpx = importlib.import_module("httpx")
            self._http_client = httpx.AsyncClient(
                timeout=self.timeout_seconds,
                follow_redirects=True,
            )
            return self._http_client
        except ImportError:
            return None

    async def _get_json_single(self, url: str) -> tuple[int, dict[str, Any]]:
        client = await self._get_http_client()
        if client is not None:
            resp = await client.get(url)
            try:
                payload = resp.json()
            except Exception:
                payload = {}
            return resp.status_code, payload

        import asyncio

        def _sync_fetch() -> tuple[int, dict[str, Any]]:
            req = Request(url, headers={"Accept": "application/json"})
            try:
                with urlopen(req, timeout=self.timeout_seconds) as resp:
                    data = json.loads(resp.read().decode("utf-8", errors="replace"))
                    return getattr(resp, "status", 200), data
            except HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                try:
                    data = json.loads(raw)
                except Exception:
                    data = {"error": {"message": raw}}
                return exc.code, data
            except URLError as exc:
                raise RuntimeError(f"YouTube API network error: {exc.reason}") from exc

        return await asyncio.to_thread(_sync_fetch)

    async def _request(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        """Execute a YouTube Data API v3 GET request with automatic key failover."""
        if not self.api_keys:
            raise YouTubeAPIQuotaError("No YOUTUBE_API_KEY configured.")

        attempts = max(1, len(self.api_keys))
        last_error = "YouTube API request failed."
        for _ in range(attempts):
            api_key = self._next_available_key()
            query_params = {k: v for k, v in params.items() if v is not None and v != ""}
            query_params["key"] = api_key
            url = f"{YOUTUBE_API_BASE}/{endpoint}?{urlencode(query_params)}"
            try:
                status, payload = await self._get_json_single(url)
            except Exception as exc:
                last_error = redact_secrets(str(exc), self.api_keys)
                continue

            if status == 200:
                return payload

            err = payload.get("error") or {}
            reasons = {
                str(item.get("reason", ""))
                for item in (err.get("errors") or [])
                if isinstance(item, dict)
            }
            msg = redact_secrets(str(err.get("message") or f"HTTP {status}"), self.api_keys)
            last_error = msg

            if status in {403, 429} or reasons & {
                "quotaExceeded",
                "dailyLimitExceeded",
                "rateLimitExceeded",
                "keyInvalid",
                "accessNotConfigured",
            }:
                logger.warning("YouTube API key rotated due to status=%s (%s)", status, msg)
                self._exhausted_keys.add(api_key)
                continue

            raise RuntimeError(f"YouTube API error ({status}): {msg}")

        raise YouTubeAPIQuotaError(last_error)

    def _video_resource_to_item(self, item: dict[str, Any]) -> YouTubeVideoItem | None:
        vid = str(item.get("id") or "").strip()
        if not vid:
            return None
        snippet = item.get("snippet") or {}
        content_details = item.get("contentDetails") or {}
        live_details = item.get("liveStreamingDetails") or {}

        title = html.unescape(str(snippet.get("title") or "Unknown Track")).strip()
        artist = html.unescape(
            str(snippet.get("channelTitle") or "Unknown Artist")
        ).strip()
        if artist.endswith(" - Topic"):
            artist = artist[: -len(" - Topic")].strip()

        live_broadcast = str(snippet.get("liveBroadcastContent") or "none").lower()
        duration = parse_iso8601_duration(content_details.get("duration"))
        is_live = live_broadcast == "live" or (
            bool(live_details.get("actualStartTime"))
            and not live_details.get("actualEndTime")
            and duration == 0
        )

        return YouTubeVideoItem(
            video_id=vid,
            title=title,
            artist=artist,
            duration_seconds=duration,
            is_live=is_live,
            webpage_url=f"https://www.youtube.com/watch?v={vid}",
            thumbnail_url=_pick_thumbnail(snippet),
            channel_id=str(snippet.get("channelId") or "") or None,
        )

    async def get_videos(self, video_ids: list[str]) -> list[YouTubeVideoItem]:
        """Batch-fetch full video metadata for up to 50 IDs per request."""
        clean_ids = [vid.strip() for vid in dict.fromkeys(video_ids) if vid and vid.strip()]
        if not clean_ids:
            return []

        results: list[YouTubeVideoItem] = []
        for idx in range(0, len(clean_ids), 50):
            batch = clean_ids[idx : idx + 50]
            payload = await self._request(
                "videos",
                {
                    "part": "snippet,contentDetails,liveStreamingDetails",
                    "id": ",".join(batch),
                    "maxResults": len(batch),
                },
            )
            for raw_item in payload.get("items") or []:
                parsed = self._video_resource_to_item(raw_item)
                if parsed is not None:
                    results.append(parsed)
        return results

    async def search_videos(
        self,
        query: str,
        limit: int = 5,
        music_category_only: bool = True,
    ) -> list[YouTubeVideoItem]:
        """Search YouTube via official Data API v3 and enrich results with exact durations."""
        max_results = max(1, min(25, limit))
        params: dict[str, Any] = {
            "part": "snippet",
            "type": "video",
            "q": query.strip(),
            "maxResults": max_results,
            "safeSearch": "none",
        }
        if self.region_code:
            params["regionCode"] = self.region_code
        if music_category_only:
            params["videoCategoryId"] = "10"

        payload = await self._request("search", params)
        items = payload.get("items") or []
        if not items and music_category_only:
            params.pop("videoCategoryId", None)
            payload = await self._request("search", params)
            items = payload.get("items") or []

        video_ids: list[str] = []
        for item in items:
            id_obj = item.get("id") or {}
            vid = str(id_obj.get("videoId") or "").strip()
            if vid:
                video_ids.append(vid)

        if not video_ids:
            return []
        enriched = await self.get_videos(video_ids)
        by_id = {item.video_id: item for item in enriched}
        return [by_id[vid] for vid in video_ids if vid in by_id]

    async def get_playlist_videos(
        self, playlist_id: str, limit: int = 20
    ) -> list[YouTubeVideoItem]:
        """Fetch playable tracks from a YouTube playlist via official Data API v3."""
        max_items = max(1, min(50, limit))
        payload = await self._request(
            "playlistItems",
            {
                "part": "contentDetails,snippet",
                "playlistId": playlist_id.strip(),
                "maxResults": max_items,
            },
        )
        video_ids: list[str] = []
        for item in payload.get("items") or []:
            content = item.get("contentDetails") or {}
            vid = str(content.get("videoId") or "").strip()
            if not vid:
                res = (item.get("snippet") or {}).get("resourceId") or {}
                vid = str(res.get("videoId") or "").strip()
            if vid:
                video_ids.append(vid)

        if not video_ids:
            return []
        enriched = await self.get_videos(video_ids)
        by_id = {item.video_id: item for item in enriched}
        return [by_id[vid] for vid in video_ids if vid in by_id]

    async def discover_autoplay(
        self,
        seed_title: str,
        seed_artist: str,
        exclude_ids: set[str],
        max_duration_seconds: int = 7200,
    ) -> YouTubeVideoItem | None:
        """Find a related music track via official YouTube Data API v3 without deprecated params."""
        search_term = clean_music_query(seed_artist, seed_title)
        candidates = await self.search_videos(
            f"{search_term} audio", limit=10, music_category_only=True
        )
        for item in candidates:
            if item.video_id in exclude_ids:
                continue
            if item.is_live:
                continue
            if 0 < item.duration_seconds <= max_duration_seconds:
                return item
        return None


@dataclass(slots=True)
class ResolvedStream:
    """Audio stream URL and optional HTTP headers returned by an external stream relay."""

    stream_url: str
    headers: dict[str, str] | None = None
    title: str | None = None
    artist: str | None = None
    duration_seconds: int | None = None
    is_live: bool | None = None


class ExternalStreamResolver:
    """Resolves playable YouTube audio streams via an external relay, Piped, or Invidious API.

    Supported URL patterns for STREAM_API_URL:
      - Custom template containing `{video_id}`, e.g. `https://api.example.com/audio?id={video_id}`
      - Piped API base or `/streams` endpoint (parses `audioStreams`)
      - Invidious API base or `/api/v1/videos` endpoint (parses `adaptiveFormats`)
      - Generic JSON API endpoint returning `stream_url`, `audio_url`, or `url`
    """

    def __init__(
        self,
        base_url: str,
        api_key: str = "",
        timeout_seconds: float = 12.0,
    ) -> None:
        self.base_url = base_url.strip().rstrip("/")
        self.api_key = api_key.strip()
        self.timeout_seconds = timeout_seconds
        self._http_client: Any = None

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    async def close(self) -> None:
        if self._http_client is not None:
            try:
                await self._http_client.aclose()
            except Exception:
                pass
            self._http_client = None

    def build_request_url(self, video_id: str, source_url: str = "") -> str:
        """Build the target endpoint URL for resolving a video ID."""
        if "{video_id}" in self.base_url or "{url}" in self.base_url:
            return self.base_url.format(
                video_id=quote(video_id, safe=""),
                url=quote(source_url, safe="") if source_url else "",
            )
        if self.base_url.endswith("/streams") or "/streams/" in self.base_url:
            return f"{self.base_url}/{video_id}"
        if "/api/v1/videos" in self.base_url:
            return f"{self.base_url}/{video_id}"
        if "?" in self.base_url:
            sep = "&" if not self.base_url.endswith(("&", "?")) else ""
            return f"{self.base_url}{sep}{urlencode({'video_id': video_id})}"
        return f"{self.base_url}?{urlencode({'video_id': video_id})}"

    async def _fetch_json(self, url: str) -> dict[str, Any]:
        headers = {"Accept": "application/json", "User-Agent": "TelegramVCMusicBot/2.0"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
            headers["X-API-Key"] = self.api_key

        try:
            httpx = importlib.import_module("httpx")
            if self._http_client is None:
                self._http_client = httpx.AsyncClient(
                    timeout=self.timeout_seconds,
                    follow_redirects=True,
                )
            resp = await self._http_client.get(url, headers=headers)
            resp.raise_for_status()
            return resp.json()
        except ImportError:
            import asyncio

            def _sync_get() -> dict[str, Any]:
                req = Request(url, headers=headers)
                with urlopen(req, timeout=self.timeout_seconds) as resp:
                    return json.loads(resp.read().decode("utf-8", errors="replace"))

            return await asyncio.to_thread(_sync_get)

    @staticmethod
    def parse_response(payload: dict[str, Any]) -> ResolvedStream | None:
        """Extract the highest-bitrate audio stream URL from custom, Piped, or Invidious JSON."""
        if not isinstance(payload, dict):
            return None

        # 1. Generic / custom stream API JSON
        direct = (
            payload.get("stream_url")
            or payload.get("audio_url")
            or payload.get("url")
            or (payload.get("data") or {}).get("stream_url")
            or (payload.get("data") or {}).get("url")
        )
        if isinstance(direct, str) and direct.startswith(("http://", "https://")):
            raw_headers = payload.get("headers") or payload.get("http_headers")
            clean_headers = (
                {str(k): str(v) for k, v in raw_headers.items()}
                if isinstance(raw_headers, dict)
                else None
            )
            dur = payload.get("duration") or payload.get("duration_seconds")
            return ResolvedStream(
                stream_url=direct,
                headers=clean_headers,
                title=payload.get("title"),
                artist=payload.get("artist") or payload.get("uploader"),
                duration_seconds=int(dur) if dur is not None else None,
                is_live=payload.get("is_live"),
            )

        # 2. Piped `/streams/{id}` format (`audioStreams`)
        audio_streams = payload.get("audioStreams")
        if isinstance(audio_streams, list) and audio_streams:
            valid = [
                s
                for s in audio_streams
                if isinstance(s, dict)
                and isinstance(s.get("url"), str)
                and s["url"].startswith(("http://", "https://"))
                and not s.get("videoOnly")
            ]
            if valid:
                valid.sort(
                    key=lambda s: (
                        "opus" in str(s.get("codec", "")).lower(),
                        int(s.get("bitrate") or 0),
                    ),
                    reverse=True,
                )
                best = valid[0]
                return ResolvedStream(
                    stream_url=best["url"],
                    title=payload.get("title"),
                    artist=payload.get("uploader"),
                    duration_seconds=int(payload["duration"])
                    if payload.get("duration") is not None
                    else None,
                    is_live=bool(payload.get("livestream")),
                )

        # 3. Invidious `/api/v1/videos/{id}` format (`adaptiveFormats`)
        adaptive = payload.get("adaptiveFormats")
        if isinstance(adaptive, list) and adaptive:
            audio_only = [
                f
                for f in adaptive
                if isinstance(f, dict)
                and str(f.get("type", "")).startswith("audio/")
                and isinstance(f.get("url"), str)
                and f["url"].startswith(("http://", "https://"))
            ]
            if audio_only:
                audio_only.sort(
                    key=lambda f: (
                        "opus" in str(f.get("type", "")).lower(),
                        int(f.get("bitrate") or 0),
                    ),
                    reverse=True,
                )
                best = audio_only[0]
                return ResolvedStream(
                    stream_url=best["url"],
                    title=payload.get("title"),
                    artist=payload.get("author"),
                    duration_seconds=int(payload["lengthSeconds"])
                    if payload.get("lengthSeconds") is not None
                    else None,
                    is_live=bool(payload.get("liveNow")),
                )

        return None

    async def resolve_video(
        self, video_id: str, source_url: str = ""
    ) -> ResolvedStream | None:
        """Resolve a YouTube video ID to a playable audio stream URL."""
        if not self.enabled or not video_id:
            return None
        target = self.build_request_url(video_id, source_url)
        payload = await self._fetch_json(target)
        return self.parse_response(payload)
