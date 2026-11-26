"""Comprehensive unit and mocked integration tests for music_bot.py and youtube_api.py."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, IntFlag, auto
import importlib.util
from pathlib import Path
import sys
import types
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import youtube_api  # noqa: E402

MUSIC_BOT_PATH = ROOT_DIR / "music_bot.py"


def _load_music_bot_module():
    spec = importlib.util.spec_from_file_location("music_bot", MUSIC_BOT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["music_bot"] = mod
    spec.loader.exec_module(mod)
    return mod


music_bot = _load_music_bot_module()


def _install_mock_telegram_modules(
    monkeypatch: pytest.MonkeyPatch,
    *,
    with_link_preview_options: bool = True,
):
    """Install lightweight mocks for pyrogram/kurigram and pytgcalls into sys.modules."""
    pyrogram = types.ModuleType("pyrogram")
    pyrogram_enums = types.ModuleType("pyrogram.enums")
    pyrogram_errors = types.ModuleType("pyrogram.errors")
    pyrogram_types = types.ModuleType("pyrogram.types")

    class ChatMemberStatus(Enum):
        OWNER = "OWNER"
        ADMINISTRATOR = "ADMINISTRATOR"
        MEMBER = "MEMBER"
        RESTRICTED = "RESTRICTED"
        LEFT = "LEFT"
        BANNED = "BANNED"

    class ChatType(Enum):
        PRIVATE = "PRIVATE"
        GROUP = "GROUP"
        SUPERGROUP = "SUPERGROUP"

    class ParseMode(Enum):
        HTML = "HTML"

    pyrogram_enums.ChatMemberStatus = ChatMemberStatus
    pyrogram_enums.ChatType = ChatType
    pyrogram_enums.ParseMode = ParseMode

    class FloodWait(Exception):
        def __init__(self, value: int = 0) -> None:
            self.value = value

    class UserNotParticipant(Exception):
        pass

    class UserAlreadyParticipant(Exception):
        pass

    class ChatAdminRequired(Exception):
        pass

    class UserBannedInChannel(Exception):
        pass

    class InviteHashExpired(Exception):
        pass

    pyrogram_errors.FloodWait = FloodWait
    pyrogram_errors.UserNotParticipant = UserNotParticipant
    pyrogram_errors.UserAlreadyParticipant = UserAlreadyParticipant
    pyrogram_errors.ChatAdminRequired = ChatAdminRequired
    pyrogram_errors.UserBannedInChannel = UserBannedInChannel
    pyrogram_errors.InviteHashExpired = InviteHashExpired

    @dataclass
    class InlineKeyboardButton:
        text: str
        callback_data: str | None = None

    @dataclass
    class InlineKeyboardMarkup:
        inline_keyboard: list[list[InlineKeyboardButton]]

    @dataclass
    class LinkPreviewOptions:
        is_disabled: bool = False

    pyrogram_types.InlineKeyboardButton = InlineKeyboardButton
    pyrogram_types.InlineKeyboardMarkup = InlineKeyboardMarkup
    if with_link_preview_options:
        pyrogram_types.LinkPreviewOptions = LinkPreviewOptions

    class FakeFilter:
        def __and__(self, other):
            return self

    class FakeFilters:
        group = FakeFilter()
        private = FakeFilter()

        @staticmethod
        def command(*args, **kwargs):
            return FakeFilter()

        @staticmethod
        def regex(*args, **kwargs):
            return FakeFilter()

    pyrogram.filters = FakeFilters()
    pyrogram.Client = MagicMock()

    pytgcalls = types.ModuleType("pytgcalls")
    pytgcalls_types = types.ModuleType("pytgcalls.types")
    pytgcalls_exceptions = types.ModuleType("pytgcalls.exceptions")
    pytgcalls_filters = types.ModuleType("pytgcalls.filters")

    class AudioQuality(Enum):
        STUDIO = (96000, 2)
        HIGH = (48000, 2)
        MEDIUM = (36000, 1)
        LOW = (24000, 1)

    class MediaStream:
        class Flags(IntFlag):
            AUTO_DETECT = auto()
            REQUIRED = auto()
            IGNORE = auto()

        def __init__(
            self,
            media_path: str,
            audio_parameters: Any = AudioQuality.HIGH,
            video_flags: Any = None,
            ffmpeg_parameters: str | None = None,
            headers: dict[str, str] | None = None,
        ) -> None:
            self.media_path = media_path
            self.audio_parameters = audio_parameters
            self.video_flags = video_flags
            self.ffmpeg_parameters = ffmpeg_parameters
            self.headers = headers

    class GroupCallConfig:
        def __init__(self, auto_start: bool = True) -> None:
            self.auto_start = auto_start

    class StreamEnded:
        class Type(IntFlag):
            AUDIO = auto()
            VIDEO = auto()

    class ChatUpdate:
        class Status(IntFlag):
            KICKED = auto()
            LEFT_GROUP = auto()
            CLOSED_VOICE_CHAT = auto()
            LEFT_CALL = KICKED | LEFT_GROUP | CLOSED_VOICE_CHAT

    class NoActiveGroupCall(Exception):
        pass

    pytgcalls_types.AudioQuality = AudioQuality
    pytgcalls_types.MediaStream = MediaStream
    pytgcalls_types.GroupCallConfig = GroupCallConfig
    pytgcalls_types.StreamEnded = StreamEnded
    pytgcalls_types.ChatUpdate = ChatUpdate
    pytgcalls_exceptions.NoActiveGroupCall = NoActiveGroupCall

    pytgcalls_filters.stream_end = lambda *a, **k: FakeFilter()
    pytgcalls_filters.chat_update = lambda *a, **k: FakeFilter()
    pytgcalls.filters = pytgcalls_filters
    pytgcalls.PyTgCalls = MagicMock()

    monkeypatch.setitem(sys.modules, "pyrogram", pyrogram)
    monkeypatch.setitem(sys.modules, "pyrogram.enums", pyrogram_enums)
    monkeypatch.setitem(sys.modules, "pyrogram.errors", pyrogram_errors)
    monkeypatch.setitem(sys.modules, "pyrogram.types", pyrogram_types)
    monkeypatch.setitem(sys.modules, "pytgcalls", pytgcalls)
    monkeypatch.setitem(sys.modules, "pytgcalls.types", pytgcalls_types)
    monkeypatch.setitem(sys.modules, "pytgcalls.exceptions", pytgcalls_exceptions)
    monkeypatch.setitem(sys.modules, "pytgcalls.filters", pytgcalls_filters)

    return pytgcalls_types, pyrogram_errors, pyrogram_enums


def test_bot_config_from_env_and_youtube_api_keys(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "API_ID=12345\n"
        "API_HASH=abc123\n"
        "BOT_TOKEN=111:TOKEN\n"
        "SESSION_STRING=\n"
        "AUDIO_QUALITY=high\n"
        "AUTOPLAY_DEFAULT=false\n"
        "MAX_QUEUE_SIZE=25\n"
        "YOUTUBE_API_KEY=key_one\n"
        "YOUTUBE_API_KEYS=key_one,key_two,key_three\n"
        "STREAM_MODE=cache\n"
        "YOUTUBE_PLAYER_CLIENTS=tv,web_safari\n"
        "PO_TOKEN_SERVER=http://127.0.0.1:4416\n",
        encoding="utf-8",
    )
    for k in (
        "API_ID",
        "API_HASH",
        "BOT_TOKEN",
        "SESSION_STRING",
        "AUDIO_QUALITY",
        "AUTOPLAY_DEFAULT",
        "MAX_QUEUE_SIZE",
        "YOUTUBE_API_KEY",
        "YOUTUBE_API_KEYS",
        "STREAM_MODE",
        "YOUTUBE_PLAYER_CLIENTS",
        "PO_TOKEN_SERVER",
    ):
        monkeypatch.delenv(k, raising=False)

    cfg = music_bot.BotConfig.from_env(env_file)
    assert cfg.api_id == 12345
    assert cfg.api_hash == "abc123"
    assert cfg.bot_token == "111:TOKEN"
    assert cfg.audio_quality == "HIGH"
    assert cfg.autoplay_default is False
    assert cfg.max_queue_size == 25
    assert cfg.youtube_api_keys == ["key_one", "key_two", "key_three"]
    assert cfg.stream_mode == "cache"
    assert cfg.youtube_player_clients == ("tv", "web_safari")
    assert cfg.po_token_server == "http://127.0.0.1:4416"
    assert cfg.missing_credentials(require_session=True) == ["SESSION_STRING"]
    assert cfg.missing_credentials(require_session=False) == []


def test_youtube_api_helpers_duration_ids_and_redaction():
    assert youtube_api.parse_iso8601_duration("PT4M13S") == 253
    assert youtube_api.parse_iso8601_duration("PT1H2M5S") == 3725
    assert youtube_api.parse_iso8601_duration("P1DT0H0M10S") == 86410
    assert youtube_api.parse_iso8601_duration("invalid") == 0

    assert youtube_api.extract_youtube_video_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    assert (
        youtube_api.extract_youtube_video_id("https://music.youtube.com/watch?v=dQw4w9WgXcQ&si=x")
        == "dQw4w9WgXcQ"
    )
    assert (
        youtube_api.extract_youtube_playlist_id("https://www.youtube.com/playlist?list=PL12345_abc")
        == "PL12345_abc"
    )
    assert (
        youtube_api.clean_music_query("Coldplay - Topic", "Viva La Vida (Official Video)")
        == "Coldplay - Topic Viva La Vida"
    )

    masked = youtube_api.redact_secrets(
        "Failed GET https://www.googleapis.com/youtube/v3/search?key=AIzaSecretKey123 via socks5://u:pass99@1.2.3.4:1080",
        ("AIzaSecretKey123",),
    )
    assert "AIzaSecretKey123" not in masked
    assert "pass99" not in masked
    assert "***" in masked


@pytest.mark.asyncio
async def test_youtube_data_api_search_quota_rotation_playlist_and_autoplay(
    monkeypatch: pytest.MonkeyPatch,
):
    client = youtube_api.YouTubeDataAPIClient(api_keys=["bad_key", "good_key"])
    seen_urls: list[str] = []

    async def fake_get_json_single(url: str):
        seen_urls.append(url)
        if "key=bad_key" in url:
            return 403, {"error": {"message": "Quota exceeded", "errors": [{"reason": "quotaExceeded"}]}}
        if "/search?" in url:
            assert "relatedToVideoId" not in url
            return 200, {
                "items": [
                    {"id": {"videoId": "vid11111111"}},
                    {"id": {"videoId": "vid22222222"}},
                ]
            }
        if "/playlistItems?" in url:
            return 200, {
                "items": [
                    {"contentDetails": {"videoId": "vid11111111"}},
                    {"snippet": {"resourceId": {"videoId": "vid22222222"}}},
                ]
            }
        if "/videos?" in url:
            return 200, {
                "items": [
                    {
                        "id": "vid11111111",
                        "snippet": {
                            "title": "Rock &amp; Roll",
                            "channelTitle": "Band - Topic",
                            "liveBroadcastContent": "none",
                            "thumbnails": {"high": {"url": "https://img.example/1.jpg"}},
                        },
                        "contentDetails": {"duration": "PT3M20S"},
                    },
                    {
                        "id": "vid22222222",
                        "snippet": {
                            "title": "Midnight Drive",
                            "channelTitle": "Synth Artist",
                            "liveBroadcastContent": "none",
                        },
                        "contentDetails": {"duration": "PT4M05S"},
                    },
                ]
            }
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(client, "_get_json_single", fake_get_json_single)

    search_res = await client.search_videos("rock", limit=2)
    assert len(search_res) == 2
    assert search_res[0].title == "Rock & Roll"
    assert search_res[0].artist == "Band"
    assert search_res[0].duration_seconds == 200
    assert search_res[0].thumbnail_url == "https://img.example/1.jpg"
    assert any("key=bad_key" in u for u in seen_urls)
    assert any("key=good_key" in u for u in seen_urls)

    playlist_res = await client.get_playlist_videos("PL999", limit=10)
    assert [v.video_id for v in playlist_res] == ["vid11111111", "vid22222222"]

    auto = await client.discover_autoplay(
        seed_title="Rock & Roll (Official Video)",
        seed_artist="Band",
        exclude_ids={"vid11111111"},
    )
    assert auto is not None
    assert auto.video_id == "vid22222222"
    await client.close()


def test_external_stream_resolver_formats():
    resolver = youtube_api.ExternalStreamResolver(
        "https://relay.example.com/streams", api_key="secret_tok"
    )
    assert resolver.build_request_url("abc12345678") == "https://relay.example.com/streams/abc12345678"

    tpl_resolver = youtube_api.ExternalStreamResolver(
        "https://worker.example.com/audio?id={video_id}"
    )
    assert tpl_resolver.build_request_url("abc12345678") == "https://worker.example.com/audio?id=abc12345678"

    # Generic JSON
    g = youtube_api.ExternalStreamResolver.parse_response(
        {"stream_url": "https://cdn.example/a.opus", "headers": {"User-Agent": "Test"}, "duration": 180}
    )
    assert g is not None and g.stream_url == "https://cdn.example/a.opus"
    assert g.headers == {"User-Agent": "Test"}

    # Piped JSON (prefers opus + highest bitrate)
    p = youtube_api.ExternalStreamResolver.parse_response(
        {
            "title": "Piped Song",
            "uploader": "Artist",
            "duration": 210,
            "audioStreams": [
                {"url": "https://p.example/aac.m4a", "codec": "mp4a.40.2", "bitrate": 128000, "videoOnly": False},
                {"url": "https://p.example/opus.webm", "codec": "opus", "bitrate": 160000, "videoOnly": False},
            ],
        }
    )
    assert p is not None and p.stream_url == "https://p.example/opus.webm"

    # Invidious JSON
    inv = youtube_api.ExternalStreamResolver.parse_response(
        {
            "title": "Inv Song",
            "author": "Inv Artist",
            "lengthSeconds": 195,
            "adaptiveFormats": [
                {"type": "video/mp4", "url": "https://v.example/v.mp4", "bitrate": "900000"},
                {"type": "audio/webm; codecs=\"opus\"", "url": "https://v.example/a.webm", "bitrate": "155000"},
            ],
        }
    )
    assert inv is not None and inv.stream_url == "https://v.example/a.webm" and inv.duration_seconds == 195


def test_validate_remote_url_and_ffmpeg_filter_builder(monkeypatch: pytest.MonkeyPatch):
    assert music_bot.validate_remote_url("https://cdn.example.com/song.mp3") == "https://cdn.example.com/song.mp3"

    for bad_url in (
        "file:///etc/passwd",
        "ftp://example.com/a.mp3",
        "http://localhost:8000/stream",
        "http://127.0.0.1/stream.mp3",
        "http://169.254.169.254/latest/meta-data/",
        "http://10.0.0.5/audio.mp3",
        "http://nas.local/music.mp3",
    ):
        with pytest.raises(ValueError):
            music_bot.validate_remote_url(bad_url)

    monkeypatch.setattr(
        music_bot.socket,
        "getaddrinfo",
        lambda host, port, proto=0: [(2, 1, 6, "", ("192.168.1.50", 0))],
    )
    with pytest.raises(ValueError):
        music_bot.validate_remote_url("https://rebind.example.com/audio.mp3")

    assert music_bot.build_ffmpeg_parameters("") is None
    assert (
        music_bot.build_ffmpeg_parameters("loudnorm=I=-14:TP=-1.5:LRA=11")
        == "--audio ---mid -af loudnorm=I=-14:TP=-1.5:LRA=11"
    )
    assert music_bot.build_ffmpeg_parameters("-af bass=g=4") == "--audio ---mid -af bass=g=4"
    assert music_bot.build_ffmpeg_parameters("--audio ---mid -af dynaudnorm") == "--audio ---mid -af dynaudnorm"


def test_formatting_and_html_escaping():
    assert music_bot.format_duration(0) == "00:00"
    assert music_bot.format_duration(125) == "02:05"
    assert music_bot.format_duration(3665) == "01:01:05"
    assert music_bot.format_duration(100, is_live=True) == "LIVE"
    bar = music_bot.format_progress_bar(60, 120, width=10)
    assert "01:00" in bar and "02:00" in bar

    cfg = music_bot.BotConfig(1, "h", "t", "s", audio_quality="STUDIO")
    state = music_bot.ChatState(chat_id=-1001, autoplay=True)
    state.now_playing = music_bot.Track(
        track_id="vid1",
        title="Rock & <Roll>",
        artist="AC/DC <Live>",
        duration_seconds=200,
        source_url="https://youtu.be/vid1",
        source_type="youtube",
        requested_by="Alice <Admin>",
        requester_id=42,
    )
    rendered = music_bot.build_now_playing_text(state, cfg)
    assert "Rock &amp; &lt;Roll&gt;" in rendered
    assert "AC/DC &lt;Live&gt;" in rendered
    assert "Alice &lt;Admin&gt;" in rendered
    assert "STUDIO" in rendered


@pytest.mark.asyncio
async def test_media_engine_uses_youtube_api_before_ytdlp_and_ttl_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    cfg = music_bot.BotConfig(
        1,
        "h",
        "t",
        "s",
        cache_dir=tmp_path,
        youtube_api_keys=["yt_api_key_1"],
    )
    engine = music_bot.MediaEngine(cfg)
    try:
        api_calls = 0

        async def fake_search_videos(query: str, limit: int = 5):
            nonlocal api_calls
            api_calls += 1
            return [
                youtube_api.YouTubeVideoItem(
                    video_id="api12345678",
                    title="Neon Pulse",
                    artist="Synth DJ",
                    duration_seconds=215,
                    is_live=False,
                    webpage_url="https://www.youtube.com/watch?v=api12345678",
                )
            ]

        monkeypatch.setattr(engine.yt_api, "search_videos", fake_search_videos)

        # Ensure yt_dlp is NOT even invoked when API key succeeds
        fake_ytdlp = types.ModuleType("yt_dlp")
        fake_ytdlp.YoutubeDL = MagicMock(side_effect=AssertionError("yt-dlp should not run for API search"))
        monkeypatch.setitem(sys.modules, "yt_dlp", fake_ytdlp)

        first = await engine.search_tracks("neon pulse", requested_by="Bob", requester_id=7, limit=5)
        second = await engine.search_tracks("NEON PULSE", requested_by="Alice", requester_id=9, limit=5)
        assert api_calls == 1
        assert len(first) == 1
        assert first[0].track_id == "api12345678"
        assert first[0].allow_alt_fallback is True
        assert second[0].requested_by == "Alice"
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_media_engine_client_rotation_soundcloud_fallback_and_stream_api(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    cfg = music_bot.BotConfig(
        1,
        "h",
        "t",
        "s",
        cache_dir=tmp_path,
        youtube_player_clients=("default", "tv"),
        alt_source_fallback=True,
        stream_mode="hybrid",
        po_token_server="http://127.0.0.1:4416",
    )
    engine = music_bot.MediaEngine(cfg)
    try:
        seen_clients: list[str | None] = []
        seen_extractor_args: list[Any] = []

        class FakeRotateYDL:
            def __init__(self, opts):
                self.opts = opts
                yt_args = (opts.get("extractor_args") or {}).get("youtube") or {}
                seen_clients.append((yt_args.get("player_client") or ["default"])[0])
                seen_extractor_args.append(opts.get("extractor_args"))

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def extract_info(self, target, download=False):
                if target.startswith("scsearch1:"):
                    return {
                        "entries": [
                            {
                                "id": "sc99",
                                "title": "Believer (SC)",
                                "uploader": "Imagine Dragons",
                                "duration": 204,
                                "webpage_url": "https://soundcloud.com/x/believer",
                                "url": "https://cf-media.sndcdn.com/believer.mp3",
                            }
                        ]
                    }
                raise RuntimeError("Sign in to confirm you're not a bot")

        fake_ytdlp = types.ModuleType("yt_dlp")
        fake_ytdlp.YoutubeDL = FakeRotateYDL
        monkeypatch.setitem(sys.modules, "yt_dlp", fake_ytdlp)

        t = music_bot.Track(
            track_id="vid_blocked",
            title="Believer",
            artist="Imagine Dragons",
            duration_seconds=204,
            source_url="https://www.youtube.com/watch?v=vid_blocked",
            source_type="youtube",
            requested_by="Bob",
            requester_id=7,
            allow_alt_fallback=True,
        )
        await engine.prepare_track(t, immediate=True)
        assert seen_clients[:2] == ["default", "tv"]
        assert "youtubepot-bgutilhttp" in (seen_extractor_args[0] or {})
        assert t.source_type == "soundcloud"
        assert t.stream_target == "https://cf-media.sndcdn.com/believer.mp3"

        # Now test external stream API priority on a fresh YouTube track
        engine.stream_api = youtube_api.ExternalStreamResolver("https://relay.example/audio?id={video_id}")
        monkeypatch.setattr(
            engine.stream_api,
            "resolve_video",
            AsyncMock(
                return_value=youtube_api.ResolvedStream(
                    stream_url="https://relay.example/media/123.opus",
                    headers={"X-Relay": "1"},
                )
            ),
        )
        t2 = music_bot.Track(
            track_id="vid_relay",
            title="Relay Song",
            artist="Relay Artist",
            duration_seconds=180,
            source_url="https://www.youtube.com/watch?v=vid_relay",
            source_type="youtube",
            requested_by="Bob",
            requester_id=7,
        )
        await engine.prepare_track(t2, immediate=True)
        assert t2.stream_target == "https://relay.example/media/123.opus"
        assert t2.stream_headers == {"X-Relay": "1"}
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_media_engine_prepare_cache_pruning_and_telegram_media(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    cfg = music_bot.BotConfig(
        1, "h", "t", "s", cache_dir=tmp_path, max_cache_files=5, stream_mode="cache"
    )
    engine = music_bot.MediaEngine(cfg)
    try:
        for i in range(6):
            (tmp_path / f"old_{i}.m4a").write_bytes(b"audio" * 10)

        class FakeDownloadYDL:
            def __init__(self, opts):
                self.opts = opts

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def extract_info(self, url, download=False):
                return {
                    "id": "track999",
                    "title": "Crystal Audio",
                    "artist": "Studio Band",
                    "duration": 180,
                    "is_live": False,
                    "url": "https://stream.example.com/audio.m4a",
                }

            def download(self, urls):
                (tmp_path / "track999.m4a").write_bytes(b"real_m4a_bytes")

        fake_ytdlp = types.ModuleType("yt_dlp")
        fake_ytdlp.YoutubeDL = FakeDownloadYDL
        monkeypatch.setitem(sys.modules, "yt_dlp", fake_ytdlp)

        t = music_bot.Track(
            track_id="track999",
            title="Crystal Audio",
            artist="Studio Band",
            duration_seconds=180,
            source_url="https://www.youtube.com/watch?v=track999",
            source_type="youtube",
            requested_by="Bob",
            requester_id=7,
        )
        await engine.prepare_track(t, immediate=True)
        assert t.stream_target == str(tmp_path / "track999.m4a")
        remaining = [f for f in tmp_path.iterdir() if f.is_file()]
        assert len(remaining) == 5

        fake_bot = MagicMock()

        async def fake_download_media(msg, file_name):
            Path(file_name).write_bytes(b"tg_voice_bytes")
            return file_name

        fake_bot.download_media = AsyncMock(side_effect=fake_download_media)
        tg_track = music_bot.Track(
            track_id="tg_uniq_1",
            title="Voice Note",
            artist="Telegram Media",
            duration_seconds=15,
            source_url="",
            source_type="telegram",
            requested_by="Bob",
            requester_id=7,
            tg_message=object(),
        )
        await engine.prepare_track(tg_track, bot_client=fake_bot)
        assert tg_track.stream_target is not None and Path(tg_track.stream_target).exists()
        assert tg_track.tg_message is None
    finally:
        await engine.aclose()


def _make_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    ffmpeg_audio_filter: str = "",
):
    pytgcalls_types, pyrogram_errors, pyrogram_enums = _install_mock_telegram_modules(monkeypatch)
    cfg = music_bot.BotConfig(
        api_id=123,
        api_hash="hash",
        bot_token="bot_tok",
        session_string="sess",
        cache_dir=tmp_path,
        audio_quality="STUDIO",
        autoplay_default=True,
        prefetch_enabled=True,
        idle_leave_seconds=0,
        ffmpeg_audio_filter=ffmpeg_audio_filter,
    )
    svc = music_bot.MusicBotService(cfg)
    svc.bot = MagicMock()
    svc.assistant = MagicMock()
    svc.calls = MagicMock()
    svc._assistant_id = 999

    sent_msg = MagicMock(id=501)
    sent_msg.edit_text = AsyncMock(return_value=sent_msg)
    svc.bot.send_message = AsyncMock(return_value=sent_msg)
    svc.bot.get_chat_member = AsyncMock(side_effect=pyrogram_errors.UserNotParticipant())
    svc.bot.get_chat = AsyncMock(return_value=MagicMock(username=None))
    svc.bot.create_chat_invite_link = AsyncMock(return_value=MagicMock(invite_link="https://t.me/+inv123"))
    svc.assistant.join_chat = AsyncMock()
    svc.assistant.get_chat = AsyncMock()
    svc.calls.play = AsyncMock()
    svc.calls.pause = AsyncMock(return_value=True)
    svc.calls.resume = AsyncMock(return_value=True)
    svc.calls.leave_call = AsyncMock()
    svc.calls.change_volume_call = AsyncMock()

    return svc, pytgcalls_types, pyrogram_enums


def _url_track(tid: str, title: str) -> music_bot.Track:
    return music_bot.Track(
        track_id=tid,
        title=title,
        artist="Artist",
        duration_seconds=200,
        source_url=f"https://cdn.example.com/{tid}.mp3",
        source_type="url",
        requested_by="Alice",
        requester_id=10,
        stream_target=f"https://cdn.example.com/{tid}.mp3",
    )


@pytest.mark.asyncio
async def test_service_kurigram_safe_send_edit_and_anonymous_admin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    svc, pytgcalls_types, pyrogram_enums = _make_service(
        tmp_path, monkeypatch, ffmpeg_audio_filter="loudnorm=I=-14:TP=-1.5:LRA=11"
    )
    svc.config.admin_only_controls = True

    msg = await svc.safe_send(-1005, "Hello", thread_id=42)
    assert msg is not None
    send_kwargs = svc.bot.send_message.call_args.kwargs
    assert "link_preview_options" in send_kwargs
    assert send_kwargs["link_preview_options"].is_disabled is True
    assert send_kwargs["message_thread_id"] == 42

    await svc.safe_edit(msg, "Updated")
    edit_kwargs = msg.edit_text.call_args.kwargs
    assert "disable_web_page_preview" not in edit_kwargs
    assert edit_kwargs["link_preview_options"].is_disabled is True

    # Anonymous group admin (sender_chat_id == chat_id) must be authorized
    assert (
        await svc.check_control_permission(
            -1005, user_id=0, track=None, admin_only=True, sender_chat_id=-1005
        )
        is True
    )

    # Existing group member warms assistant peer cache for in_memory session
    svc.bot.get_chat_member = AsyncMock(
        return_value=MagicMock(status=pyrogram_enums.ChatMemberStatus.MEMBER, is_member=True)
    )
    await svc.ensure_assistant_in_chat(-1005)
    svc.assistant.get_chat.assert_awaited_once_with(-1005)

    stream = svc._build_media_stream(_url_track("f1", "Normalized"))
    assert stream.ffmpeg_parameters == "--audio ---mid -af loudnorm=I=-14:TP=-1.5:LRA=11"
    assert stream.audio_parameters == pytgcalls_types.AudioQuality.STUDIO
    await svc.shutdown()


@pytest.mark.asyncio
async def test_service_play_queue_playlist_auto_skip_and_autoplay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    svc, pytgcalls_types, _ = _make_service(tmp_path, monkeypatch)
    monkeypatch.setattr(svc.media, "find_autoplay_track", AsyncMock(return_value=_url_track("auto1", "Auto Song")))

    t1 = _url_track("t1", "First Track")
    t2 = _url_track("t2", "Second Track")
    t3 = _url_track("t3", "Priority Track")

    outcome1, pos1 = await svc.enqueue_track(-1001, t1)
    assert (outcome1, pos1) == ("playing", 0)
    svc.assistant.join_chat.assert_awaited_once_with("https://t.me/+inv123")
    svc.calls.play.assert_awaited_once()
    media_stream = svc.calls.play.call_args.args[1]
    assert media_stream.audio_parameters == pytgcalls_types.AudioQuality.STUDIO
    assert media_stream.video_flags == pytgcalls_types.MediaStream.Flags.IGNORE

    assert await svc.enqueue_track(-1001, t2) == ("queued", 1)
    assert await svc.enqueue_track(-1001, t3, play_next=True) == ("queued", 1)

    state = svc.get_chat(-1001)
    assert [t.track_id for t in state.queue] == ["t3", "t2"]

    # Broken queued track auto-skips to the following working track instead of stopping VC
    broken = music_bot.Track(
        track_id="bad_vid",
        title="Broken",
        artist="X",
        duration_seconds=120,
        source_url="https://www.youtube.com/watch?v=bad_vid",
        source_type="youtube",
        requested_by="Alice",
        requester_id=10,
    )
    state.queue.appendleft(broken)

    orig_prepare = svc.media.prepare_track

    async def flaky_prepare(track, **kwargs):
        if track.track_id == "bad_vid":
            raise RuntimeError("Video unavailable")
        return await orig_prepare(track, **kwargs)

    monkeypatch.setattr(svc.media, "prepare_track", flaky_prepare)
    await svc.handle_stream_ended(-1001)
    assert state.now_playing.track_id == "t3"
    assert state.in_call is True

    async with state.lock:
        assert await svc.toggle_pause_locked(state, want_pause=True) == "paused"
        assert state.is_paused is True
        assert await svc.toggle_pause_locked(state, want_pause=False) == "resumed"
        assert state.is_paused is False

    async with state.lock:
        assert svc.set_loop_locked(state, "track") == "track"
    await svc.handle_stream_ended(-1001)
    assert state.now_playing.track_id == "t3"

    async with state.lock:
        assert svc.set_loop_locked(state, "off") == "off"
    await svc.handle_stream_ended(-1001)
    assert state.now_playing.track_id == "t2"

    if state.prefetch_task is not None:
        await state.prefetch_task
    await svc.handle_stream_ended(-1001)
    assert state.now_playing.track_id == "auto1"

    async with state.lock:
        assert svc.set_autoplay_locked(state, False) is False
    await svc.handle_stream_ended(-1001)
    assert state.now_playing is None

    # Playlist enqueue on idle starts track 1 and preserves order
    started, queued = await svc.enqueue_playlist(
        -1001, [_url_track("p1", "P1"), _url_track("p2", "P2"), _url_track("p3", "P3")]
    )
    assert started is True and queued == 2
    assert state.now_playing.track_id == "p1"
    assert [t.track_id for t in state.queue] == ["p2", "p3"]

    await svc.handle_left_call(-1001)
    assert state.in_call is False
    await svc.shutdown()


@pytest.mark.asyncio
async def test_service_shuffle_queue_loop_and_shutdown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    svc, _, _ = _make_service(tmp_path, monkeypatch)
    svc.config.prefetch_enabled = False

    await svc.enqueue_track(-1002, _url_track("a", "A"))
    await svc.enqueue_track(-1002, _url_track("b", "B"))
    await svc.enqueue_track(-1002, _url_track("c", "C"))

    state = svc.get_chat(-1002)
    async with state.lock:
        assert svc.shuffle_queue_locked(state) is True
        svc.set_loop_locked(state, "queue")

    await svc.handle_stream_ended(-1002)
    assert any(t.track_id == "a" for t in state.queue)

    await svc.shutdown()
    svc.calls.leave_call.assert_awaited_with(-1002)


@pytest.mark.asyncio
async def test_service_remote_stream_failure_falls_back_to_local_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    svc, _, _ = _make_service(tmp_path, monkeypatch)
    track = music_bot.Track(
        track_id="yt_fallback1",
        title="Fallback Song",
        artist="Artist",
        duration_seconds=180,
        source_url="https://www.youtube.com/watch?v=yt_fallback1",
        source_type="youtube",
        requested_by="Alice",
        requester_id=10,
    )

    local_file = tmp_path / "yt_fallback1.opus"
    local_file.write_bytes(b"opus_data")

    async def fake_prepare(tr, bot_client=None, immediate=False, force_local_cache=False):
        if force_local_cache:
            tr.stream_target = str(local_file)
            tr.stream_headers = None
        else:
            tr.stream_target = "https://googlevideo.example/403-stream"
            tr.stream_headers = {"User-Agent": "Test"}
        return tr

    play_calls = 0

    async def fake_play(chat_id, stream, config=None):
        nonlocal play_calls
        play_calls += 1
        if play_calls == 1:
            raise RuntimeError("HTTP 403 opening remote stream")

    monkeypatch.setattr(svc.media, "prepare_track", fake_prepare)
    svc.calls.play = AsyncMock(side_effect=fake_play)

    outcome, _ = await svc.enqueue_track(-1003, track)
    assert outcome == "playing"
    assert play_calls == 2
    assert track.stream_target == str(local_file)
    await svc.shutdown()


@pytest.mark.asyncio
async def test_media_engine_api_quota_falls_back_to_ytdlp_search(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    cfg = music_bot.BotConfig(
        1,
        "h",
        "t",
        "s",
        cache_dir=tmp_path,
        youtube_api_keys=["exhausted_key"],
    )
    engine = music_bot.MediaEngine(cfg)
    try:
        monkeypatch.setattr(
            engine.yt_api,
            "search_videos",
            AsyncMock(side_effect=youtube_api.YouTubeAPIQuotaError("quotaExceeded")),
        )

        class FakeFallbackYDL:
            def __init__(self, opts):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def extract_info(self, target, download=False):
                return {
                    "entries": [
                        {
                            "id": "fb123456789",
                            "title": "Fallback Search Track",
                            "uploader": "Fallback Artist",
                            "duration": 195,
                        }
                    ]
                }

        fake_ytdlp = types.ModuleType("yt_dlp")
        fake_ytdlp.YoutubeDL = FakeFallbackYDL
        monkeypatch.setitem(sys.modules, "yt_dlp", fake_ytdlp)

        res = await engine.search_tracks("ambient", requested_by="Bob", requester_id=5, limit=1)
        assert len(res) == 1
        assert res[0].track_id == "fb123456789"
    finally:
        await engine.aclose()

