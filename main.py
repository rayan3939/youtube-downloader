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

def get_base_ydl_opts():
    """Return base yt-dlp options with cookies if available."""
    opts = {
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "extractor_args": {
            "youtube": {
                "player_client": ["android", "ios", "tv"],
            }
        }
    }
    if HAS_COOKIES:
        opts["cookiefile"] = COOKIES_FILE
    return opts

async def get_active_cobalt_instances():
    """Fetch online cobalt instances from instances.cobalt.best."""
    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            r = await client.get("https://instances.cobalt.best/api/instances.json")
            if r.status_code == 200:
                instances = r.json()
                online_apis = []
                for inst in instances:
                    online_val = inst.get("online")
                    is_online = False
                    if isinstance(online_val, bool):
                        is_online = online_val
                    elif isinstance(online_val, dict):
                        is_online = online_val.get("api") == True or online_val.get("status") == "up"
                    elif isinstance(online_val, int):
                        is_online = online_val == 1
                    else:
                        is_online = online_val is not False
                    
                    if is_online:
                        api = inst.get("api")
                        if api:
                            if api.endswith("/"):
                                api = api[:-1]
                            score = inst.get("score", 0)
                            online_apis.append({"api": api, "score": score})
                
                # Sort by score descending
                online_apis.sort(key=lambda x: x["score"], reverse=True)
                return [x["api"] for x in online_apis]
    except Exception as e:
        logger.error(f"Error fetching active Cobalt instances: {str(e)}")
    
    # Fallback list if fetching fails
    return [
        "https://co.wuk.sh",
        "https://cobalt.api.rylor.com",
        "https://api.cobalt.tools"
    ]

async def download_via_cobalt(url: str, format_type: str, quality: str):
    """Attempt to get a download stream URL from active Cobalt instances."""
    instances = await get_active_cobalt_instances()
    logger.info(f"Retrieved {len(instances)} active Cobalt instances to try.")
    
    payload = {
        "url": url,
        "videoQuality": quality if format_type == "mp4" else "1080",
        "audioFormat": "mp3",
        "downloadMode": "audio" if format_type == "mp3" else "auto",
        "audioOnly": True if format_type == "mp3" else False,
        "isAudioOnly": True if format_type == "mp3" else False,
        "filenamePattern": "basic"
    }
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    
    for idx, api_base in enumerate(instances[:10]):
        logger.info(f"Trying Cobalt instance {idx+1}/{min(len(instances), 10)}: {api_base}")
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.post(api_base, headers=headers, json=payload)
                if res.status_code == 200:
                    data = res.json()
                    status = data.get("status")
                    if status in ["redirect", "tunnel", "success"]:
                        stream_url = data.get("url")
                        if stream_url:
                            logger.info(f"✅ Success using Cobalt instance {api_base}: {stream_url[:80]}...")
                            filename = data.get("filename", "video")
                            return stream_url, filename
                    elif status == "picker":
                        picker_items = data.get("picker", [])
                        if picker_items and isinstance(picker_items, list):
                            first_item_url = picker_items[0].get("url")
                            if first_item_url:
                                logger.info(f"✅ Success (picker) using Cobalt instance {api_base}")
                                return first_item_url, "media"
                    logger.warning(f"Cobalt instance {api_base} returned status '{status}'. Response: {res.text[:200]}")
                else:
                    logger.warning(f"Cobalt instance {api_base} returned status code {res.status_code}. Response: {res.text[:200]}")
        except Exception as e:
            logger.warning(f"Cobalt instance {api_base} failed: {str(e)}")
            
    return None, None

def get_video_id(url: str):
    """Extract YouTube video ID from URL."""
    pattern = r'(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:[^\/\n\s]+\/\S+\/|(?:v|e(?:mbed)?)\/|\S*?[?&]v=)|youtu\.be\/|youtube-nocookie\.com\/embed\/)([a-zA-Z0-9_-]{11})'
    match = re.search(pattern, url)
    if match:
        return match.group(1)
    return None

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
            async with httpx.AsyncClient(timeout=5.0) as client:
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
        async with httpx.AsyncClient(timeout=4.0) as client:
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
                        "quality": str(height),
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
            
        # 2. Try Piped API
        meta = await fetch_metadata_via_piped(video_id)
        
        # 3. Try OEmbed API if Piped failed
        if not meta:
            meta = await fetch_metadata_via_oembed(req.url, video_id)
            
        if not meta:
            logger.error("All metadata fallback systems failed.")
            raise HTTPException(status_code=500, detail=f"YouTube blocked metadata extraction and all fallback providers failed: {str(e)}")
            
        # Construct standard fallback format list
        duration = meta.get("duration", 0)
        fallback_formats = [
            {"quality": "1080", "label": "1080p (Full HD)", "ext": "mp4", "size": 0},
            {"quality": "720", "label": "720p (HD)", "ext": "mp4", "size": 0},
            {"quality": "480", "label": "480p", "ext": "mp4", "size": 0},
            {"quality": "360", "label": "360p", "ext": "mp4", "size": 0},
            {"quality": "240", "label": "240p", "ext": "mp4", "size": 0},
            {"quality": "144", "label": "144p", "ext": "mp4", "size": 0}
        ]
        
        if duration > 0:
            bitrate_map = {
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
    """Download, process, and stream video/audio back to the client."""
    tmp_dir = tempfile.gettempdir()
    tmp_id = str(uuid.uuid4())
    tmp_path = os.path.join(tmp_dir, tmp_id)

    out_file = None
    media_type = None
    filename_ext = None

    # Define progress hook closure
    def make_progress_hook(download_id):
        if download_id not in progress_store:
            progress_store[download_id] = {"status": "starting", "progress": 0}

        def hook(d):
            state = progress_store.get(download_id, {"status": "starting", "progress": 0})

            if d['status'] == 'downloading':
                total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
                downloaded = d.get('downloaded_bytes', 0)
                percent = downloaded / total if total > 0 else 0

                info_dict = d.get('info_dict', {}) or {}
                vcodec = info_dict.get('vcodec') or 'none'
                acodec = info_dict.get('acodec') or 'none'

                is_video = (vcodec != 'none')
                is_audio = (acodec != 'none')

                if is_video and is_audio:
                    progress = 5 + int(percent * 80)
                elif is_video and not is_audio:
                    progress = 5 + int(percent * 65)
                elif is_audio and not is_video:
                    if req.format == "mp4":
                        progress = 70 + int(percent * 15)
                    else:
                        progress = 5 + int(percent * 80)
                else:
                    progress = 5 + int(percent * 80)

                current_p = state.get("progress", 0)
                if progress > current_p:
                    state["progress"] = progress
                state["status"] = "downloading"

            elif d['status'] == 'finished':
                current_p = state.get("progress", 0)
                if req.format == "mp4" and current_p <= 70:
                    state["progress"] = 70
                    state["status"] = "downloading"
                else:
                    state["progress"] = 90
                    state["status"] = "processing"

            progress_store[download_id] = state
        return hook

    try:
        # Initialize progress store entry
        progress_store[req.download_id] = {"status": "starting", "progress": 0}

        if req.format == "mp3":
            ydl_opts = {
                **get_base_ydl_opts(),
                "format": "bestaudio/best",
                "outtmpl": tmp_path + ".%(ext)s",
                "postprocessors": [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": req.quality,
                }],
                "concurrent_fragment_downloads": 5,
                "socket_timeout": 30,
                "retries": 3,
                "postprocessor_args": {
                    "ffmpeg": ["-threads", "4", "-preset", "ultrafast"]
                },
            }
            out_file = tmp_path + ".mp3"
            media_type = "audio/mpeg"
            filename_ext = "mp3"
        else:
            ydl_opts = {
                **get_base_ydl_opts(),
                "format": f"bestvideo[ext=mp4][height<={req.quality}]+bestaudio[ext=m4a]/bestvideo[height<={req.quality}]+bestaudio/best",
                "outtmpl": tmp_path + ".%(ext)s",
                "merge_output_format": "mp4",
                "recode_video": "mp4",
                "concurrent_fragment_downloads": 5,
                "socket_timeout": 30,
                "retries": 3,
                "postprocessor_args": {
                    "VideoConvertor+ffmpeg": [
                        "-threads", "4",
                        "-c:v", "libx264",
                        "-crf", "20",
                        "-preset", "ultrafast",
                        "-c:a", "aac",
                        "-b:a", "192k"
                    ]
                },
            }
            out_file = tmp_path + ".mp4"
            media_type = "video/mp4"
            filename_ext = "mp4"

        # Attach progress hook
        ydl_opts["progress_hooks"] = [make_progress_hook(req.download_id)]

        logger.info(f"Starting download: {req.url} format={req.format} quality={req.quality}")

        def run_download():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(req.url, download=True)

        info = await asyncio.to_thread(run_download)

        if not info:
            raise Exception("Download returned no data")

        title = info.get("title", "video")

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

        # Stream file to client with cleanup
        def iterfile():
            try:
                with open(out_file, "rb") as f:
                    while chunk := f.read(1048576):  # 1MB chunks
                        yield chunk
            except Exception as e:
                logger.error(f"Error during file streaming: {str(e)}")
            finally:
                if os.path.exists(out_file):
                    try:
                        os.remove(out_file)
                        logger.info(f"Cleaned up temp file: {out_file}")
                    except Exception as ex:
                        logger.error(f"Failed to remove temp file {out_file}: {str(ex)}")
                if req.download_id in progress_store:
                    try:
                        del progress_store[req.download_id]
                    except Exception:
                        pass

        # Safety net cleanup
        background_tasks.add_task(cleanup_temp_file, out_file)

        file_size = os.path.getsize(out_file)
        headers = {
            "Content-Disposition": f'attachment; filename="{safe_title}.{filename_ext}"',
            "Content-Length": str(file_size),
            "Access-Control-Expose-Headers": "Content-Disposition, Content-Length"
        }

        return StreamingResponse(
            iterfile(),
            media_type=media_type,
            headers=headers
        )

    except Exception as e:
        logger.warning(f"yt-dlp download failed ({str(e)}), entering Cobalt fallback rotation...")
        progress_store[req.download_id] = {"status": "processing", "progress": 8}
        
        try:
            stream_url, cobalt_filename = await download_via_cobalt(req.url, req.format, req.quality)
            if not stream_url:
                raise Exception("All Cobalt instance download attempts failed.")
                
            media_type = "audio/mpeg" if req.format == "mp3" else "video/mp4"
            filename_ext = req.format
            safe_title = "".join(c for c in (cobalt_filename or "video") if c.isalnum() or c in " -_").strip()
            if not safe_title:
                safe_title = "video"
                
            progress_store[req.download_id] = {"status": "streaming", "progress": 10}
            
            client = httpx.AsyncClient(timeout=60.0)
            response = await client.send(client.build_request("GET", stream_url), stream=True)
            
            if response.status_code >= 400:
                await response.aclose()
                await client.aclose()
                raise Exception(f"Cobalt stream URL returned status code {response.status_code}")
                
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
