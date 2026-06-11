from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl, validator
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import yt_dlp
import os
import uuid
import re
import tempfile
import logging
import asyncio
import time
import httpx
import random


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cookie file path — baked into the Docker image for cloud deployment
COOKIES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies.txt")
HAS_COOKIES = os.path.exists(COOKIES_FILE)

if HAS_COOKIES:
    logger.info(f"✅ Cookies file found at {COOKIES_FILE}")
else:
    logger.warning("⚠️ No cookies.txt found — YouTube may block requests from datacenter IPs")

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Agency Downloader API", version="2.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# YouTube URL validation regex
YOUTUBE_REGEX = re.compile(
    r'(?:youtube\.com|youtu\.be|youtube-nocookie\.com)',
    re.IGNORECASE
)

class VideoRequest(BaseModel):
    url: str

    @validator('url')
    def validate_youtube_url(cls, v):
        v_str = str(v).strip()
        if not YOUTUBE_REGEX.search(v_str):
            raise ValueError("Invalid YouTube URL. Please provide a valid YouTube watch, shorts, playlist, or share link.")
        return v_str

class DownloadRequest(BaseModel):
    url: str
    format: str       # "mp4" or "mp3"
    quality: str      # quality height (e.g. "1080", "720") or bitrate (e.g. "320", "192")
    download_id: str

    @validator('url')
    def validate_youtube_url(cls, v):
        v_str = str(v).strip()
        if not YOUTUBE_REGEX.search(v_str):
            raise ValueError("Invalid YouTube URL.")
        return v_str

    @validator('format')
    def validate_format(cls, v):
        if v not in ["mp4", "mp3"]:
            raise ValueError("Format must be 'mp4' or 'mp3'.")
        return v

progress_store = {}


def make_progress_hook(download_id: str):
    """Create a yt-dlp progress hook that updates progress_store."""
    def hook(d):
        if d.get("status") == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            downloaded = d.get("downloaded_bytes", 0)
            if total > 0:
                pct = int((downloaded / total) * 100)
                progress_store[download_id] = {"status": "downloading", "progress": pct}
        elif d.get("status") == "finished":
            progress_store[download_id] = {"status": "processing", "progress": 95}
    return hook


# Standard browser headers used across all outgoing requests
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

def get_base_ydl_opts():
    """Return base yt-dlp options with cookies if available."""
    opts = {
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "extractor_args": {
            "youtube": {
                "player_client": ["all"],
            }
        },
    }
    # Enable aria2c for fast multi-connection downloads if available
    import shutil
    if shutil.which("aria2c"):
        opts["external_downloader"] = "aria2c"
        opts["external_downloader_args"] = {"default": ["-x", "16", "-k", "1M", "-j", "16", "--file-allocation=none"]}
    if HAS_COOKIES:
        opts["cookiefile"] = COOKIES_FILE
    return opts


async def get_active_invidious_instances():
    """Fetch online invidious instances from api.invidious.io."""
    try:
        logger.info("Fetching active instances from api.invidious.io...")
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"}
        async with httpx.AsyncClient(timeout=6.0, headers=headers) as client:
            r = await client.get("https://api.invidious.io/instances.json")
            if r.status_code == 200:
                instances = r.json()
                online_domains = []
                for item in instances:
                    domain = item[0]
                    details = item[1]
                    if details.get("type") == "https":
                        monitor = details.get("monitor")
                        uptime = 0
                        if monitor and isinstance(monitor, dict):
                            uptime = monitor.get("uptime", 0) or 0
                        if uptime > 85:
                            online_domains.append(domain)
                if online_domains:
                    logger.info(f"Successfully retrieved {len(online_domains)} active Invidious instances.")
                    random.shuffle(online_domains)
                    return online_domains
    except Exception as e:
        logger.error(f"Error fetching Invidious instances: {str(e)}")
    
    return [
        "inv.thepixora.com",
        "invidious.nerdvpn.de",
        "invidious.f5.si",
        "yt.chocolatemoo53.com",
        "inv.nadeko.net"
    ]

async def fetch_video_info_via_invidious(video_id: str):
    domains = await get_active_invidious_instances()
    for domain in domains[:6]:
        url = f"https://{domain}/api/v1/videos/{video_id}"
        logger.info(f"Trying Invidious instance for info: {domain}")
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"}
            async with httpx.AsyncClient(timeout=8.0, headers=headers) as client:
                res = await client.get(url)
                if res.status_code == 200:
                    data = res.json()
                    if data.get("formatStreams") or data.get("adaptiveFormats"):
                        logger.info(f"✅ Success fetching video info from Invidious: {domain}")
                        return data
        except Exception as e:
            logger.warning(f"Invidious instance {domain} failed: {str(e)}")
    return None

async def fetch_metadata_via_invidious(video_id: str):
    """Fetch video metadata and formats from active Invidious instances."""
    data = await fetch_video_info_via_invidious(video_id)
    if not data:
        return None
    try:
        title = data.get("title", "YouTube Video")
        thumbnail = f"https://img.youtube.com/vi/{video_id}/maxresdefault.jpg"
        if data.get("videoThumbnails"):
            thumbnail = data["videoThumbnails"][0].get("url", thumbnail)
            
        duration = data.get("lengthSeconds", 0)
        channel = data.get("author", "Unknown Channel")
        
        height_formats = {}
        
        for f in data.get("formatStreams", []):
            q_label = f.get("qualityLabel")
            if q_label:
                height_match = re.search(r'(\d+)', q_label)
                if height_match:
                    height = int(height_match.group(1))
                    clen = f.get("clen")
                    size = int(clen) if clen and str(clen).isdigit() else 0
                    height_formats[height] = {
                        "quality": str(height),
                        "label": f"{height}p",
                        "ext": "mp4",
                        "size": size
                    }
                    
        best_audio_size = 0
        for f in data.get("adaptiveFormats", []):
            mime = f.get("type", "")
            if mime.startswith("audio/"):
                size = int(f.get("clen", 0)) or 0
                if size > best_audio_size:
                    best_audio_size = size
                    
        for f in data.get("adaptiveFormats", []):
            mime = f.get("type", "")
            if mime.startswith("video/"):
                q_label = f.get("qualityLabel")
                if q_label:
                    height_match = re.search(r'(\d+)', q_label)
                    if height_match:
                        height = int(height_match.group(1))
                        vsize = int(f.get("clen", 0)) or 0
                        total_size = vsize + best_audio_size
                        
                        if height not in height_formats or total_size > height_formats[height]["size"]:
                            label = f"{height}p"
                            if height == 4320:
                                label += " (8K Ultra HD)"
                            elif height == 2160:
                                label += " (4K Ultra HD)"
                            elif height == 1440:
                                label += " (2K)"
                            elif height == 1080:
                                label += " (Full HD)"
                            elif height == 720:
                                label += " (HD)"
                                
                            height_formats[height] = {
                                "quality": str(height),
                                "label": label,
                                "ext": "mp4",
                                "size": total_size
                            }
                            
        formats = list(height_formats.values())
        formats.sort(key=lambda x: int(x["quality"]), reverse=True)
        
        if formats:
            return {
                "title": title,
                "thumbnail": thumbnail,
                "duration": duration,
                "channel": channel,
                "formats": formats
            }
    except Exception as e:
        logger.error(f"Error parsing Invidious metadata: {str(e)}")
    return None

async def download_file_async(url, dest_path, download_id, start_pct, end_pct):
    """Download a URL to dest_path asynchronously with parallel chunked downloading for speed."""
    logger.info(f"Downloading from stream to {dest_path}...")
    
    # First, get the total file size with a HEAD request
    async with httpx.AsyncClient(timeout=30.0, headers=BROWSER_HEADERS) as client:
        head_resp = await client.head(url, follow_redirects=True)
        total_bytes = int(head_resp.headers.get("Content-Length", 0))
        supports_range = head_resp.headers.get("Accept-Ranges", "").lower() == "bytes"
    
    NUM_PARALLEL = 4
    CHUNK_READ_SIZE = 2 * 1024 * 1024  # 2MB read chunks
    
    if total_bytes > 5_000_000 and supports_range:
        # Parallel chunked download for large files
        logger.info(f"Using {NUM_PARALLEL}-connection parallel download for {total_bytes} bytes")
        segment_size = total_bytes // NUM_PARALLEL
        ranges = []
        for i in range(NUM_PARALLEL):
            start = i * segment_size
            end = (i + 1) * segment_size - 1 if i < NUM_PARALLEL - 1 else total_bytes - 1
            ranges.append((start, end))
        
        segment_data = [None] * NUM_PARALLEL
        downloaded_total = [0]  # mutable for closure
        
        async def download_segment(idx, byte_start, byte_end):
            range_header = {**BROWSER_HEADERS, "Range": f"bytes={byte_start}-{byte_end}"}
            async with httpx.AsyncClient(timeout=120.0) as seg_client:
                async with seg_client.stream("GET", url, headers=range_header) as r:
                    parts = []
                    async for chunk in r.aiter_bytes(chunk_size=CHUNK_READ_SIZE):
                        parts.append(chunk)
                        downloaded_total[0] += len(chunk)
                        if total_bytes > 0:
                            pct = start_pct + (downloaded_total[0] / total_bytes) * (end_pct - start_pct)
                            progress_store[download_id] = {"status": "downloading", "progress": int(pct)}
                    segment_data[idx] = b"".join(parts)
        
        await asyncio.gather(*[
            download_segment(i, s, e) for i, (s, e) in enumerate(ranges)
        ])
        
        with open(dest_path, "wb") as f:
            for seg in segment_data:
                if seg:
                    f.write(seg)
    else:
        # Standard single-connection download for small files or servers without range support
        async with httpx.AsyncClient(timeout=120.0, headers=BROWSER_HEADERS) as client:
            async with client.stream("GET", url, headers=BROWSER_HEADERS, follow_redirects=True) as r:
                if r.status_code >= 400:
                    raise Exception(f"Stream returned status code {r.status_code}")
                
                if total_bytes == 0:
                    total_bytes = int(r.headers.get("Content-Length", 0))
                downloaded = 0
                
                with open(dest_path, "wb") as f:
                    async for chunk in r.aiter_bytes(chunk_size=CHUNK_READ_SIZE):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_bytes > 0:
                            pct = start_pct + (downloaded / total_bytes) * (end_pct - start_pct)
                            progress_store[download_id] = {"status": "downloading", "progress": int(pct)}

def get_best_audio_stream_url(invidious_data):
    """Extract best audio stream URL from Invidious video data."""
    adaptive = invidious_data.get("adaptiveFormats", [])
    audio_streams = []
    for f in adaptive:
        mime = f.get("type", "")
        if mime.startswith("audio/"):
            audio_streams.append(f)
            
    if not audio_streams:
        for f in invidious_data.get("formatStreams", []):
            if f.get("url"):
                return f["url"]
        return None
        
    audio_streams.sort(key=lambda x: int(x.get("bitrate") or 0), reverse=True)
    return audio_streams[0]["url"]

def get_video_and_audio_stream_urls(invidious_data, quality):
    """Extract video and audio stream URLs for specified quality from Invidious data."""
    adaptive = invidious_data.get("adaptiveFormats", [])
    audio_url = get_best_audio_stream_url(invidious_data)
    
    video_streams = []
    for f in adaptive:
        mime = f.get("type", "")
        if mime.startswith("video/"):
            video_streams.append(f)
            
    if not video_streams:
        raise Exception("No video streams found in Invidious data.")
        
    target_q = int(quality)
    best_match = None
    min_diff = float("inf")
    
    for f in video_streams:
        q_label = f.get("qualityLabel", "")
        height_match = re.search(r'(\d+)', q_label)
        if height_match:
            height = int(height_match.group(1))
            diff = abs(height - target_q)
            if diff < min_diff:
                min_diff = diff
                best_match = f
                
    if not best_match:
        best_match = video_streams[0]
        
    return best_match["url"], audio_url

async def get_streams_via_piped(video_id: str, format_type: str, quality: str):
    """Query Piped instances for video/audio stream URLs, preferring adaptive streams for correct quality."""
    piped_instances = [
        "https://api.piped.private.coffee",
        "https://pipedapi.kavin.rocks",
        "https://pipedapi.leptons.xyz",
        "https://pipedapi.adminforge.de",
        "https://pipedapi.owo.si",
        "https://pipedapi.ducks.party"
    ]
    random.shuffle(piped_instances)
    
    for api_base in piped_instances[:4]:
        url = f"{api_base}/streams/{video_id}"
        logger.info(f"Trying Piped instance for streaming: {api_base}")
        try:
            async with httpx.AsyncClient(timeout=8.0, headers=BROWSER_HEADERS) as client:
                res = await client.get(url)
                if res.status_code == 200:
                    data = res.json()
                    
                    if format_type == "mp3":
                        audio_streams = data.get("audioStreams", [])
                        if audio_streams:
                            audio_streams.sort(key=lambda x: int(x.get("bitrate") or 0), reverse=True)
                            return audio_streams[0]["url"], None
                            
                    else:
                        video_streams = data.get("videoStreams", [])
                        audio_streams = data.get("audioStreams", [])
                        
                        if not video_streams:
                            continue
                        
                        target_q = int(quality)
                        
                        # ALWAYS prefer adaptive (videoOnly) streams for correct quality
                        # Progressive streams are typically limited to 360p/720p
                        adaptive_videos = [f for f in video_streams if f.get("videoOnly")]
                        progressive_videos = [f for f in video_streams if not f.get("videoOnly")]
                        
                        best_video = None
                        use_adaptive = False
                        
                        # Try adaptive streams first (these have correct quality up to 8K)
                        if adaptive_videos:
                            # Prefer MP4/webm, find closest quality
                            min_diff = float("inf")
                            for f in adaptive_videos:
                                q_label = f.get("quality", "")
                                height_match = re.search(r'(\d+)', q_label)
                                if height_match:
                                    height = int(height_match.group(1))
                                    diff = abs(height - target_q)
                                    if diff < min_diff or (diff == min_diff and f.get("mimeType", "").startswith("video/mp4")):
                                        min_diff = diff
                                        best_video = f
                                        use_adaptive = True
                        
                        # Fall back to progressive if no adaptive match
                        if not best_video and progressive_videos:
                            min_diff = float("inf")
                            for f in progressive_videos:
                                q_label = f.get("quality", "")
                                height_match = re.search(r'(\d+)', q_label)
                                if height_match:
                                    height = int(height_match.group(1))
                                    diff = abs(height - target_q)
                                    if diff < min_diff:
                                        min_diff = diff
                                        best_video = f
                        
                        if not best_video:
                            best_video = video_streams[0]
                            use_adaptive = best_video.get("videoOnly", False)
                        
                        # If using adaptive stream, we need a separate audio stream
                        best_audio_url = None
                        if use_adaptive and audio_streams:
                            audio_streams.sort(key=lambda x: int(x.get("bitrate") or 0), reverse=True)
                            best_audio_url = audio_streams[0]["url"]
                        elif not use_adaptive:
                            best_audio_url = None  # progressive has audio built in
                            
                        logger.info(f"Piped: selected {'adaptive' if use_adaptive else 'progressive'} stream, quality={best_video.get('quality')}")
                        return best_video["url"], best_audio_url
        except Exception as e:
            logger.warning(f"Piped instance {api_base} streaming check failed: {str(e)}")
            
    return None, None


async def get_active_cobalt_instances():
    """Return a short hard‑coded list of reliable Cobalt instances.
    The full scraper is retained for completeness but we will normally use only the first
    instance (the fastest known) for high‑resolution downloads.
    """
    # Fast‑track list – first entry is the primary instance
    return [
        {"api": "https://cobaltapi.kittycat.boo", "frontend": "https://cobalt.kittycat.boo", "version": "11.7.1"},
        {"api": "https://nuko-c.meowing.de", "frontend": "https://cobalt.meowing.de", "version": "11.7.1"},
        {"api": "https://cobalt.alpha.wolfy.love", "frontend": "https://cobalt.canine.tools", "version": "11.7.1"},
        {"api": "https://cobalt.omega.wolfy.love", "frontend": "https://cobalt.canine.tools", "version": "11.7.1"},
        {"api": "https://melon.clxxped.lol", "frontend": "https://cobalt.clxxped.lol", "version": "11.7.1"},
        {"api": "https://lime.clxxped.lol", "frontend": "https://cobalt.clxxped.lol", "version": "11.7.1"},
        {"api": "https://subito-c.meowing.de", "frontend": "https://cobalt.meowing.de", "version": "11.7.1"},
        {"api": "https://api.qwkuns.me", "frontend": "https://qwkuns.me", "version": "11.7.1"},
    ]

async def download_via_cobalt_one(url: str, format_type: str, quality: str):
    """Fast path: use the primary hard‑coded Cobalt instance only.
    Returns a tuple (stream_url, filename) or (None, None) on failure.
    """
    # Primary instance – known to work for 8K/4K streams
    primary = {
        "api": "https://cobaltapi.kittycat.boo",
        "frontend": "https://cobalt.kittycat.boo",
        "version": "11.7.1",
    }
    api_base = primary["api"]
    frontend_url = primary["frontend"]
    version = primary["version"]

    logger.info(f"Attempting Cobalt fast‑path with {api_base}")

    payload_v10 = {
        "url": url,
        "videoQuality": quality if format_type == "mp4" else "1080",
        "audioFormat": "mp3",
        "downloadMode": "audio" if format_type == "mp3" else "auto",
    }
    payload_v7 = {
        "url": url,
        "videoQuality": quality if format_type == "mp4" else "1080",
        "audioFormat": "mp3",
        "audioOnly": True if format_type == "mp3" else False,
    }
    candidates = [payload_v10, payload_v7] if version.startswith("11") else [payload_v7, payload_v10]
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": BROWSER_HEADERS["User-Agent"],
        "Origin": frontend_url,
        "Referer": f"{frontend_url}/",
    }
    for p_idx, payload in enumerate(candidates):
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.post(api_base, headers=headers, json=payload)
                if res.status_code == 200:
                    data = res.json()
                    status = data.get("status")
                    if status in ["redirect", "tunnel", "success"]:
                        stream_url = data.get("url")
                        if stream_url:
                            logger.info(f"✅ Cobalt fast‑path success: {stream_url[:80]}...")
                            return stream_url, data.get("filename", "video")
                    elif status == "picker":
                        picker = data.get("picker", [])
                        if picker:
                            return picker[0].get("url"), "media"
                    logger.warning(f"Cobalt fast‑path returned status '{status}'.")
                else:
                    logger.warning(f"Cobalt fast‑path HTTP {res.status_code}")
        except Exception as e:
            logger.warning(f"Cobalt fast‑path error: {str(e)}")
    return None, None


async def download_via_cobalt(url: str, format_type: str, quality: str):
    """Wrapper that calls download_via_cobalt_one (kept for backward compatibility)."""
    return await download_via_cobalt_one(url, format_type, quality)


def get_video_id(url: str):
    """Extract YouTube video ID from URL."""
    pattern = r'(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:[^\/\n\s]+\/\S+\/|(?:v|e(?:mbed)?)\/|\S*?[?&]v=)|youtu\.be\/|youtube-nocookie\.com\/embed\/)([a-zA-Z0-9_-]{11})'
    match = re.search(pattern, url)
    if match:
        return match.group(1)
    return None

# Helper to download 8K/4K video using yt-dlp with aria2c for maximum speed
async def download_8k_video(url: str, cookies_path: str, output_dir: str, target_height: int = 4320, download_id: str = None) -> str:
    """Download high-res video using yt-dlp with aria2c multi-connection acceleration.
    Tries progressively lower resolutions if the target isn't available.
    """
    import shutil
    
    ydl_opts = {
        "outtmpl": os.path.join(output_dir, "%(title)s.%(ext)s"),
        "format": f"bestvideo[height<={target_height}]+bestaudio/best[height<={target_height}]/best",
        "merge_output_format": "mp4",
        "nocheckcertificate": True,
        "extractor_args": {
            "youtube": {
                "player_client": ["all"],
            }
        },
        "quiet": True,
        "no_warnings": True,
    }
    if cookies_path and os.path.exists(cookies_path):
        ydl_opts["cookiefile"] = cookies_path
    # Use aria2c for blazing fast parallel downloads
    if shutil.which("aria2c"):
        ydl_opts["external_downloader"] = "aria2c"
        ydl_opts["external_downloader_args"] = {"default": ["-x", "16", "-k", "1M", "-j", "16", "--file-allocation=none"]}
    # Add progress hook if download_id provided
    if download_id:
        ydl_opts["progress_hooks"] = [make_progress_hook(download_id)]

    def _run():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if not info:
                raise Exception("yt-dlp returned no info for 8K download")
            filename = ydl.prepare_filename(info)
            # yt-dlp may change extension after merge
            if not os.path.exists(filename):
                base = os.path.splitext(filename)[0]
                for ext in [".mp4", ".mkv", ".webm"]:
                    if os.path.exists(base + ext):
                        filename = base + ext
                        break
            if not os.path.exists(filename):
                # Search output_dir for any recently created file
                files = sorted(
                    [os.path.join(output_dir, f) for f in os.listdir(output_dir)],
                    key=os.path.getmtime, reverse=True
                )
                if files:
                    filename = files[0]
                else:
                    raise FileNotFoundError(f"Downloaded 8K file not found in {output_dir}")
            return filename
    return await asyncio.to_thread(_run)

async def fetch_metadata_via_piped(video_id: str):
    """Fetch video metadata using a public Piped instance."""
    piped_instances = [
        "https://api.piped.private.coffee",
        "https://pipedapi.kavin.rocks",
        "https://pipedapi.leptons.xyz",
        "https://pipedapi.adminforge.de",
        "https://pipedapi.owo.si",
        "https://pipedapi.ducks.party"
    ]
    random.shuffle(piped_instances)
    
    for api_base in piped_instances[:4]:
        url = f"{api_base}/streams/{video_id}"
        logger.info(f"Trying Piped instance to fetch metadata: {api_base}")
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"}
            async with httpx.AsyncClient(timeout=5.0, headers=headers) as client:
                res = await client.get(url)
                if res.status_code == 200:
                    data = res.json()
                    title = data.get("title", "YouTube Video")
                    channel = data.get("uploader", "Unknown Channel")
                    duration = data.get("duration", 0)
                    thumbnail = data.get("thumbnailUrl") or f"https://img.youtube.com/vi/{video_id}/maxresdefault.jpg"
                    
                    logger.info(f"✅ Success fetching metadata from Piped: {title}")
                    return {
                        "title": title,
                        "thumbnail": thumbnail,
                        "duration": duration,
                        "channel": channel
                    }
        except Exception as e:
            logger.warning(f"Piped instance {api_base} metadata fetch failed: {str(e)}")
    return None

async def fetch_metadata_via_oembed(url: str, video_id: str):
    """Fetch basic video metadata using YouTube's official public OEmbed API."""
    oembed_url = f"https://www.youtube.com/oembed?url={url}&format=json"
    logger.info(f"Fetching metadata via YouTube OEmbed: {oembed_url}")
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"}
        async with httpx.AsyncClient(timeout=4.0, headers=headers) as client:
            res = await client.get(oembed_url)
            if res.status_code == 200:
                data = res.json()
                title = data.get("title", "YouTube Video")
                channel = data.get("author_name", "Unknown Channel")
                thumbnail = data.get("thumbnail_url") or f"https://img.youtube.com/vi/{video_id}/maxresdefault.jpg"
                return {
                    "title": title,
                    "thumbnail": thumbnail,
                    "duration": 0,
                    "channel": channel
                }
    except Exception as e:
        logger.error(f"YouTube OEmbed metadata fetch failed: {str(e)}")
    return None


# ──────────────────────────────────────────────────────────────────────────────

# Health & Progress endpoints
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/progress/{download_id}")
async def get_progress(download_id: str):
    data = progress_store.get(download_id, {"status": "starting", "progress": 0})
    return JSONResponse(content=data)

@app.get("/api/health")
async def health_check():
    return {
        "status": "ok",
        "message": "Agency API is running smoothly.",
        "cookies": HAS_COOKIES,
    }

import urllib.request
@app.get("/api/test_ip")
async def test_ip():
    try:
        req = urllib.request.Request("https://www.youtube.com/", headers={'User-Agent': 'Mozilla/5.0'})
        res = urllib.request.urlopen(req, timeout=5)
        return {"status": "success", "length": len(res.read())}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/test_client")
async def test_client(client_name: str, use_cookies: bool = False):
    class CustomLogger:
        def __init__(self):
            self.lines = []
        def debug(self, msg):
            self.lines.append(f"[DEBUG] {msg}")
        def info(self, msg):
            self.lines.append(f"[INFO] {msg}")
        def warning(self, msg):
            self.lines.append(f"[WARNING] {msg}")
        def error(self, msg):
            self.lines.append(f"[ERROR] {msg}")

    clog = CustomLogger()
    try:
        ydl_opts = {
            "verbose": True,
            "skip_download": True,
            "logger": clog,
            "extractor_args": {
                "youtube": {
                    "player_client": [client_name],
                }
            }
        }
        if use_cookies and HAS_COOKIES:
            ydl_opts["cookiefile"] = COOKIES_FILE
            
        def run_info():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info("https://www.youtube.com/watch?v=FvDWCXN_dDs", download=False)
        
        info = await asyncio.to_thread(run_info)
        return {"status": "success", "client": client_name, "title": info.get("title"), "logs": clog.lines}
    except Exception as e:
        return {"status": "error", "client": client_name, "message": str(e), "logs": clog.lines}




# ──────────────────────────────────────────────────────────────────────────────
# /api/info — Fetch video metadata
# ──────────────────────────────────────────────────────────────────────────────

@app.post("/api/info")
@limiter.limit("15/minute")
async def get_video_info(req: VideoRequest, request: Request):
    """Fetch video metadata and format choices without downloading."""
    try:
        ydl_opts = {
            **get_base_ydl_opts(),
            "skip_download": True,
            "extract_flat": False,
            "socket_timeout": 12,
            "retries": 2,
        }

        logger.info(f"Fetching info for: {req.url} (cookies={HAS_COOKIES})")

        def run_info():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(req.url, download=False)

        info = await asyncio.to_thread(run_info)

        if not info:
            raise Exception("yt-dlp returned no data")

        # Get duration for size estimation
        duration = info.get("duration", 0)

        # Find best audio format and calculate its size/bitrate
        best_audio_size = 0
        best_audio_tbr = 128  # default fallback
        for f in info.get("formats", []):
            if f.get("acodec") != "none" and f.get("vcodec") == "none":
                audio_size = f.get("filesize") or f.get("filesize_approx") or 0
                audio_tbr = f.get("tbr") or 0
                if audio_size > best_audio_size:
                    best_audio_size = audio_size
                if audio_tbr > best_audio_tbr:
                    best_audio_tbr = audio_tbr

        if best_audio_size == 0 and duration > 0:
            best_audio_size = int((best_audio_tbr * 1000 / 8) * duration)

        # Group formats by snapped standard height to prevent duplicates
        height_formats = {}

        standards = [
            (4320, "8K Ultra HD"),
            (2160, "4K Ultra HD"),
            (1440, "2K"),
            (1080, "Full HD"),
            (720, "HD"),
            (480, ""),
            (360, ""),
            (240, ""),
            (144, "")
        ]

        for f in info.get("formats", []):
            height = f.get("height")
            if height and f.get("vcodec") != "none":
                # Find the standard snapped height (e.g. 1074 -> 1080)
                best_std, tag = min(standards, key=lambda x: abs(x[0] - height))

                # Use standard if within 25% threshold
                bucket_height = best_std if abs(best_std - height) <= 0.25 * best_std else height

                # Estimate video stream size
                vsize = f.get("filesize") or f.get("filesize_approx") or 0
                tbr = f.get("tbr")
                if vsize == 0 and tbr and duration > 0:
                    vsize = int((tbr * 1000 / 8) * duration)

                total_size = vsize
                if f.get("acodec") == "none":
                    total_size += best_audio_size

                if total_size == 0 and duration > 0:
                    bitrate_map = {
                        4320: 30000 * 1024 / 8,
                        2160: 15000 * 1024 / 8,
                        1440: 6000 * 1024 / 8,
                        1080: 3000 * 1024 / 8,
                        720: 1500 * 1024 / 8,
                        480: 800 * 1024 / 8,
                        360: 400 * 1024 / 8,
                        240: 250 * 1024 / 8,
                        144: 100 * 1024 / 8,
                    }
                    bitrate = bitrate_map.get(bucket_height, 1000 * 1024 / 8)
                    total_size = int(bitrate * duration)

                # Keep the format with the largest size for this bucket
                existing = height_formats.get(bucket_height)
                if not existing or total_size > existing["size"]:
                    label = f"{bucket_height}p"
                    if bucket_height == 4320:
                        label += " (8K Ultra HD)"
                    elif bucket_height == 2160:
                        label += " (4K Ultra HD)"
                    elif bucket_height == 1440:
                        label += " (2K)"
                    elif bucket_height == 1080:
                        label += " (Full HD)"
                    elif bucket_height == 720:
                        label += " (HD)"

                    height_formats[bucket_height] = {
                        "quality": str(bucket_height),
                        "label": label,
                        "ext": "mp4",
                        "size": total_size
                    }

        formats = list(height_formats.values())
        formats.sort(key=lambda x: int(x["quality"]), reverse=True)

        return {
            "title": info.get("title"),
            "thumbnail": info.get("thumbnail"),
            "duration": info.get("duration"),
            "channel": info.get("uploader"),
            "formats": formats,
        }
    except Exception as e:
        logger.warning(f"yt-dlp info fetch failed ({str(e)}), entering fallback flow...")
        
        # 1. Extract video ID
        video_id = get_video_id(req.url)
        if not video_id:
            logger.error(f"Could not extract video ID from URL: {req.url}")
            raise HTTPException(status_code=500, detail=f"Invalid YouTube URL: {str(e)}")
            
        # 1.5 Try Invidious API first
        meta = await fetch_metadata_via_invidious(video_id)
        
        # 2. Try Piped API if Invidious failed
        if not meta:
            meta = await fetch_metadata_via_piped(video_id)
        
        # 3. Try OEmbed API if Piped failed
        if not meta:
            meta = await fetch_metadata_via_oembed(req.url, video_id)
            
        if not meta:
            logger.error("All metadata fallback systems failed.")
            raise HTTPException(status_code=500, detail=f"YouTube blocked metadata extraction and all fallback providers failed: {str(e)}")
            
        # Construct standard fallback format list (including 8K/4K/2K)
        duration = meta.get("duration", 0)
        fallback_formats = [
            {"quality": "4320", "label": "4320p (8K Ultra HD)", "ext": "mp4", "size": 0},
            {"quality": "2160", "label": "2160p (4K Ultra HD)", "ext": "mp4", "size": 0},
            {"quality": "1440", "label": "1440p (2K)", "ext": "mp4", "size": 0},
            {"quality": "1080", "label": "1080p (Full HD)", "ext": "mp4", "size": 0},
            {"quality": "720", "label": "720p (HD)", "ext": "mp4", "size": 0},
            {"quality": "480", "label": "480p", "ext": "mp4", "size": 0},
            {"quality": "360", "label": "360p", "ext": "mp4", "size": 0},
            {"quality": "240", "label": "240p", "ext": "mp4", "size": 0},
            {"quality": "144", "label": "144p", "ext": "mp4", "size": 0}
        ]
        
        if duration > 0:
            bitrate_map = {
                4320: 80000 * 1024 / 8,
                2160: 30000 * 1024 / 8,
                1440: 12000 * 1024 / 8,
                1080: 3000 * 1024 / 8,
                720: 1500 * 1024 / 8,
                480: 800 * 1024 / 8,
                360: 400 * 1024 / 8,
                240: 250 * 1024 / 8,
                144: 100 * 1024 / 8,
            }
            for fmt in fallback_formats:
                q = int(fmt["quality"])
                bitrate = bitrate_map.get(q, 1000 * 1024 / 8)
                fmt["size"] = int(bitrate * duration)
                
        return {
            "title": meta["title"],
            "thumbnail": meta["thumbnail"],
            "duration": duration,
            "channel": meta["channel"],
            "formats": fallback_formats,
        }



# ──────────────────────────────────────────────────────────────────────────────
# /api/download — Download, process, and stream video/audio
# ──────────────────────────────────────────────────────────────────────────────

@app.post("/api/download")
@limiter.limit("10/minute")
async def download_video(req: DownloadRequest, request: Request, background_tasks: BackgroundTasks):
    """Download video/audio using the optimal backend service and stream it back.
    Supports high-resolution (8K/4K) via yt-dlp+aria2c and lower resolutions via Piped/Invidious.
    """
    tmp_dir = tempfile.gettempdir()
    tmp_id = str(uuid.uuid4())
    tmp_path = os.path.join(tmp_dir, tmp_id)
    
    # ── 8K/4K FAST PATH: Use dedicated high-res downloader first ──
    if req.format == "mp4" and int(req.quality) >= 2160:
        try:
            logger.info(f"🎬 8K/4K fast path: {req.url} quality={req.quality}")
            progress_store[req.download_id] = {"status": "downloading", "progress": 5}
            output_dir = os.path.join(tmp_dir, f"8k_{tmp_id}")
            os.makedirs(output_dir, exist_ok=True)
            out_file = await download_8k_video(
                req.url, COOKIES_FILE, output_dir,
                target_height=int(req.quality),
                download_id=req.download_id
            )
            title = os.path.splitext(os.path.basename(out_file))[0]
            safe_title = "".join(c for c in title if c.isalnum() or c in " -_").strip() or "video"
            file_size = os.path.getsize(out_file)
            filename_ext = os.path.splitext(out_file)[1].lstrip('.') or "mp4"
            media_type = f"video/{filename_ext}" if filename_ext != "mkv" else "video/x-matroska"
            
            progress_store[req.download_id] = {"status": "streaming", "progress": 100}
            headers = {
                "Content-Disposition": f'attachment; filename="{safe_title}.{filename_ext}"',
                "Content-Length": str(file_size),
                "Access-Control-Expose-Headers": "Content-Disposition, Content-Length"
            }
            background_tasks.add_task(cleanup_temp_file, out_file)

            def iterfile_8k():
                try:
                    with open(out_file, "rb") as f:
                        while chunk := f.read(8 * 1024 * 1024):  # 8MB chunks for large files
                            yield chunk
                finally:
                    if os.path.exists(out_file):
                        try:
                            os.remove(out_file)
                        except Exception:
                            pass
                    # Clean up the temp directory
                    import shutil as _shutil
                    try:
                        _shutil.rmtree(output_dir, ignore_errors=True)
                    except Exception:
                        pass
                    if req.download_id in progress_store:
                        try:
                            del progress_store[req.download_id]
                        except Exception:
                            pass

            logger.info(f"✅ 8K/4K download success: {safe_title} ({file_size / 1024 / 1024:.1f} MB)")
            return StreamingResponse(iterfile_8k(), media_type=media_type, headers=headers)
        except Exception as e_8k:
            logger.warning(f"8K/4K fast path failed ({str(e_8k)}), falling back to standard download...")
            # Clean up failed 8K attempt
            try:
                import shutil as _shutil
                _shutil.rmtree(os.path.join(tmp_dir, f"8k_{tmp_id}"), ignore_errors=True)
            except Exception:
                pass

    # ── STANDARD DOWNLOAD PATH ──
    try:
        # Build yt-dlp options based on request
        ydl_opts = {**get_base_ydl_opts(), "outtmpl": tmp_path + ".%(ext)s"}
        if req.format == "mp3":
            ydl_opts.update({
                "format": "bestaudio/best",
                "postprocessors": [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": req.quality,
                }],
            })
        else:
            ydl_opts.update({
                "format": f"bestvideo[ext=mp4][height<={req.quality}]+bestaudio[ext=m4a]/bestvideo[height<={req.quality}]+bestaudio/best",
                "merge_output_format": "mp4",
                "recode_video": "mp4",
            })
        ydl_opts["progress_hooks"] = [make_progress_hook(req.download_id)]

        logger.info(f"Starting download: {req.url} format={req.format} quality={req.quality}")

        def run_download():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(req.url, download=True)

        info = await asyncio.to_thread(run_download)
        if not info:
            raise Exception("Download returned no data")
        title = info.get("title", "video")
        # Determine output file and media type
        out_file = tmp_path + f".{info.get('ext', req.format)}"
        filename_ext = os.path.splitext(out_file)[1].lstrip('.')
        media_type = f"video/{filename_ext}" if filename_ext != "mp3" else "audio/mpeg"

        # Update progress to streaming status
        progress_store[req.download_id] = {"status": "streaming", "progress": 100}

        # Clean title for content disposition header
        safe_title = "".join(c for c in title if c.isalnum() or c in " -_").strip()
        if not safe_title:
            safe_title = "video"

        # Verify the file exists
        if not os.path.exists(out_file):
            files = [f for f in os.listdir(tmp_dir) if f.startswith(tmp_id)]
            if files:
                out_file = os.path.join(tmp_dir, files[0])
                filename_ext = files[0].split(".")[-1]
                if filename_ext == "mp3":
                    media_type = "audio/mpeg"
                else:
                    media_type = f"video/{filename_ext}" if filename_ext != "mkv" else "video/x-matroska"
            else:
                raise FileNotFoundError("Downloaded file could not be found.")

        # Safety net cleanup
        background_tasks.add_task(cleanup_temp_file, out_file)

        file_size = os.path.getsize(out_file)
        headers = {
            "Content-Disposition": f'attachment; filename="{safe_title}.{filename_ext}"',
            "Content-Length": str(file_size),
            "Access-Control-Expose-Headers": "Content-Disposition, Content-Length"
        }

        def iterfile():
            try:
                with open(out_file, "rb") as f:
                    while chunk := f.read(4 * 1024 * 1024):
                        yield chunk
            except Exception as e:
                logger.error(f"Error during file streaming: {str(e)}")
            finally:
                if os.path.exists(out_file):
                    try:
                        os.remove(out_file)
                        logger.info(f"Cleaned up temp file: {out_file}")
                    except Exception:
                        pass
                if req.download_id in progress_store:
                    try:
                        del progress_store[req.download_id]
                    except Exception:
                        pass
        return StreamingResponse(
            iterfile(),
            media_type=media_type,
            headers=headers
        )

    except Exception as e:
        # yt-dlp standard path failed, fall back to Invidious/Piped/Cobalt
        logger.warning(f"yt-dlp download failed ({str(e)}), entering self-healing streaming/download fallbacks...")
        progress_store[req.download_id] = {"status": "processing", "progress": 5}
        
        try:
            video_id = get_video_id(req.url)
            if not video_id:
                raise Exception("Invalid YouTube URL.")
                
            stream_url1 = None
            stream_url2 = None
            title = "video"
            
            # 1. Try Invidious first
            try:
                invidious_data = await fetch_video_info_via_invidious(video_id)
                if invidious_data:
                    title = invidious_data.get("title", "video")
                    if req.format == "mp3":
                        stream_url1 = get_best_audio_stream_url(invidious_data)
                    else:
                        target_q = int(req.quality)
                        if target_q <= 720:
                            for f in invidious_data.get("formatStreams", []):
                                q_label = f.get("qualityLabel", "")
                                height_match = re.search(r'(\d+)', q_label)
                                if height_match and int(height_match.group(1)) == target_q:
                                    stream_url1 = f.get("url")
                                    break
                        if not stream_url1:
                            stream_url1, stream_url2 = get_video_and_audio_stream_urls(invidious_data, req.quality)
            except Exception as inv_err:
                logger.warning(f"Invidious extraction failed: {str(inv_err)}")
                
            # 2. Try Piped if Invidious failed
            if not stream_url1:
                try:
                    logger.info("Invidious failed, trying Piped streams...")
                    stream_url1, stream_url2 = await get_streams_via_piped(video_id, req.format, req.quality)
                except Exception as piped_err:
                    logger.warning(f"Piped extraction failed: {str(piped_err)}")
                    
            # 3. Try Cobalt if Piped failed
            if not stream_url1:
                try:
                    logger.info("Piped failed, trying Cobalt fallback...")
                    stream_url1, cobalt_filename = await download_via_cobalt(req.url, req.format, req.quality)
                    if cobalt_filename:
                        title = cobalt_filename
                except Exception as cob_err:
                    logger.warning(f"Cobalt extraction failed: {str(cob_err)}")
                    
            if not stream_url1:
                raise Exception("All video stream extraction fallbacks (Invidious, Piped, Cobalt) failed.")
                
            # Now handle streaming/processing
            if req.format == "mp3":
                # Download audio stream to temp file
                temp_audio_input = tmp_path + "_input"
                await download_file_async(stream_url1, temp_audio_input, req.download_id, 10, 70)
                
                progress_store[req.download_id] = {"status": "processing", "progress": 75}
                out_file = tmp_path + ".mp3"
                
                # Run ffmpeg conversion to MP3
                cmd = [
                    "ffmpeg", "-y", "-i", temp_audio_input,
                    "-vn", "-ar", "44100", "-ac", "2",
                    "-b:a", f"{req.quality}k", out_file
                ]
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                if os.path.exists(temp_audio_input):
                    os.remove(temp_audio_input)
                    
                if proc.returncode != 0:
                    raise Exception(f"FFmpeg audio conversion failed: {stderr.decode()}")
                    
            else: # mp4
                if stream_url2 is None:
                    # Single progressive stream: stream directly (proxy stream) without local storage!
                    media_type = "video/mp4"
                    filename_ext = "mp4"
                    safe_title = "".join(c for c in title if c.isalnum() or c in " -_").strip()
                    if not safe_title:
                        safe_title = "video"
                        
                    progress_store[req.download_id] = {"status": "streaming", "progress": 10}
                    
                    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"}
                    client = httpx.AsyncClient(timeout=60.0, headers=headers)
                    response = await client.send(client.build_request("GET", stream_url1), stream=True)
                    
                    if response.status_code >= 400:
                        await response.aclose()
                        await client.aclose()
                        raise Exception(f"Stream URL returned status code {response.status_code}")
                        
                    file_size = response.headers.get("Content-Length")
                    headers = {
                        "Content-Disposition": f'attachment; filename="{safe_title}.{filename_ext}"',
                        "Access-Control-Expose-Headers": "Content-Disposition, Content-Length"
                    }
                    if file_size:
                        headers["Content-Length"] = file_size
                        
                    async def iter_response():
                        try:
                            total_bytes = int(file_size) if file_size else 0
                            downloaded = 0
                            async for chunk in response.aiter_bytes():
                                downloaded += len(chunk)
                                if total_bytes > 0:
                                    percent = 10 + int((downloaded / total_bytes) * 90)
                                    progress_store[req.download_id] = {"status": "streaming", "progress": percent}
                                yield chunk
                        finally:
                            await response.aclose()
                            await client.aclose()
                            if req.download_id in progress_store:
                                try:
                                    del progress_store[req.download_id]
                                except Exception:
                                    pass
                                    
                    return StreamingResponse(
                        iter_response(),
                        media_type=media_type,
                        headers=headers
                    )
                else:
                    # Adaptive video + audio: download both and merge
                    temp_video = tmp_path + "_video"
                    temp_audio = tmp_path + "_audio"
                    
                    await asyncio.gather(
                        download_file_async(stream_url1, temp_video, req.download_id, 10, 50),
                        download_file_async(stream_url2, temp_audio, req.download_id, 50, 80)
                    )
                    
                    progress_store[req.download_id] = {"status": "processing", "progress": 85}
                    out_file = tmp_path + ".mp4"
                    
                    # Merge using ffmpeg
                    cmd = [
                        "ffmpeg", "-y", "-i", temp_video, "-i", temp_audio,
                        "-c:v", "copy", "-c:a", "aac", out_file
                    ]
                    proc = await asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    stdout, stderr = await proc.communicate()
                    
                    if os.path.exists(temp_video):
                        os.remove(temp_video)
                    if os.path.exists(temp_audio):
                        os.remove(temp_audio)
                        
                    if proc.returncode != 0:
                        raise Exception(f"FFmpeg merging failed: {stderr.decode()}")
            
            # Stream the processed out_file
            media_type = "audio/mpeg" if req.format == "mp3" else "video/mp4"
            filename_ext = req.format
            safe_title = "".join(c for c in title if c.isalnum() or c in " -_").strip()
            if not safe_title:
                safe_title = "video"
                
            progress_store[req.download_id] = {"status": "streaming", "progress": 100}
            
            def iterfile():
                try:
                    with open(out_file, "rb") as f:
                        while chunk := f.read(4 * 1024 * 1024):  # 4MB chunks
                            yield chunk
                finally:
                    if os.path.exists(out_file):
                        try:
                            os.remove(out_file)
                        except Exception:
                            pass
                    if req.download_id in progress_store:
                        try:
                            del progress_store[req.download_id]
                        except Exception:
                            pass
                            
            file_size = os.path.getsize(out_file)
            headers = {
                "Content-Disposition": f'attachment; filename="{safe_title}.{filename_ext}"',
                "Content-Length": str(file_size),
                "Access-Control-Expose-Headers": "Content-Disposition, Content-Length"
            }
            
            background_tasks.add_task(cleanup_temp_file, out_file)
            
            return StreamingResponse(
                iterfile(),
                media_type=media_type,
                headers=headers
            )
            
        except Exception as fallback_err:
            logger.error(f"Fallback download failed: {str(fallback_err)}")
            if req.download_id in progress_store:
                try:
                    del progress_store[req.download_id]
                except Exception:
                    pass
            cleanup_temp_files_by_id(tmp_dir, tmp_id)
            raise HTTPException(status_code=500, detail=f"Download failed: {str(fallback_err)} (Original error: {str(e)})")


# GET endpoint mirrors the POST for mobile browser compatibility
@app.get("/api/download")
@limiter.limit("10/minute")
async def get_download_video(
    url: str,
    format: str,
    quality: str,
    download_id: str,
    request: Request,
    background_tasks: BackgroundTasks
):
    """GET endpoint for downloading/streaming media (mobile browser compatible)."""
    try:
        req = DownloadRequest(url=url, format=format, quality=quality, download_id=download_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return await download_video(req, request, background_tasks)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers & Utility Routes
# ──────────────────────────────────────────────────────────────────────────────

def cleanup_temp_file(file_path: str):
    """Clean up helper for background task."""
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            logger.info(f"Background cleanup removed file: {file_path}")
        except Exception as e:
            logger.error(f"Background cleanup failed for {file_path}: {str(e)}")

def cleanup_temp_files_by_id(directory: str, file_id: str):
    """Finds and removes any temporary files matching the uuid prefix."""
    try:
        for f in os.listdir(directory):
            if f.startswith(file_id):
                file_path = os.path.join(directory, f)
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Cleaned up error leftover: {file_path}")
    except Exception as e:
        logger.error(f"Failed to clean up error files: {str(e)}")

class DmcaRequest(BaseModel):
    name: str
    email: str
    url: str
    description: str

@app.post("/api/dmca")
@limiter.limit("5/minute")
async def submit_dmca(req: DmcaRequest, request: Request):
    logger.info(f"DMCA Request received from {req.name} ({req.email}) for URL: {req.url}. Description: {req.description}")
    return {"status": "success", "message": "DMCA request submitted successfully. We will review it within 48 hours."}
