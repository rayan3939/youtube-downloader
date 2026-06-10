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

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Agency Downloader API", version="1.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to the frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# YouTube URL validation regex - simplified and flexible to prevent false negatives
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

@app.get("/api/progress/{download_id}")
async def get_progress(download_id: str):
    data = progress_store.get(download_id, {"status": "starting", "progress": 0})
    return JSONResponse(content=data)

@app.get("/api/health")
async def health_check():
    return {"status": "ok", "message": "Agency API is running smoothly."}

@app.post("/api/info")
@limiter.limit("10/minute")
async def get_video_info(req: VideoRequest, request: Request):
    """Fetch video metadata and format choices without downloading."""
    try:
        ydl_opts = {
            "quiet": True,
            "skip_download": True,
            "no_warnings": True,
            "extract_flat": False,
            "nocheckcertificate": True,
            "extractor_args": {
                "youtube": {
                    "player_client": ["android", "ios"],
                }
            }
        }
        
        def run_info():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(req.url, download=False)
                
        info = await asyncio.to_thread(run_info)
        
        if not info:
            raise HTTPException(status_code=400, detail="Could not extract video information.")

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

        # Group formats by height, keeping the highest quality stream for size estimation
        height_formats = {}
        for f in info.get("formats", []):
            height = f.get("height")
            if height and f.get("vcodec") != "none":
                # Estimate video stream size
                vsize = f.get("filesize") or f.get("filesize_approx") or 0
                tbr = f.get("tbr")
                
                # If size is missing but bitrate (tbr) and duration are present, calculate size
                if vsize == 0 and tbr and duration > 0:
                    vsize = int((tbr * 1000 / 8) * duration)
                
                total_size = vsize
                # If format is video-only, add the best audio size
                if f.get("acodec") == "none":
                    total_size += best_audio_size

                # Secondary fallback if both size and tbr are missing
                if total_size == 0 and duration > 0:
                    bitrate_map = {
                        4320: 30000 * 1024 / 8,  # 30 Mbps
                        2160: 15000 * 1024 / 8,  # 15 Mbps
                        1440: 6000 * 1024 / 8,   # 6 Mbps
                        1080: 3000 * 1024 / 8,   # 3 Mbps
                        720: 1500 * 1024 / 8,    # 1.5 Mbps
                        480: 800 * 1024 / 8,     # 800 Kbps
                        360: 400 * 1024 / 8,     # 400 Kbps
                        240: 250 * 1024 / 8,     # 250 Kbps
                        144: 100 * 1024 / 8,     # 100 Kbps
                    }
                    bitrate = bitrate_map.get(height, 1000 * 1024 / 8)
                    total_size = int(bitrate * duration)

                # Keep the format with the largest size/bitrate for this resolution height
                existing = height_formats.get(height)
                if not existing or total_size > existing["size"]:
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
        # Sort formats highest resolution first
        formats.sort(key=lambda x: int(x["quality"]), reverse=True)
        
        return {
            "title": info.get("title"),
            "thumbnail": info.get("thumbnail"),
            "duration": info.get("duration"),
            "channel": info.get("uploader"),
            "formats": formats,
        }
    except Exception as e:
        logger.error(f"Error fetching info: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Failed to fetch video information: {str(e)}")

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
                    # Combined stream (single download)
                    progress = 5 + int(percent * 80) # 5% to 85%
                elif is_video and not is_audio:
                    # Video-only stream
                    progress = 5 + int(percent * 65) # 5% to 70%
                elif is_audio and not is_video:
                    if req.format == "mp4":
                        # Audio stream of a video+audio download
                        progress = 70 + int(percent * 15) # 70% to 85%
                    else:
                        # MP3 download
                        progress = 5 + int(percent * 80) # 5% to 85%
                else:
                    progress = 5 + int(percent * 80)
                
                # Monotonically increasing progress check
                current_p = state.get("progress", 0)
                if progress > current_p:
                    state["progress"] = progress
                state["status"] = "downloading"
                
            elif d['status'] == 'finished':
                current_p = state.get("progress", 0)
                if req.format == "mp4" and current_p <= 70:
                    state["progress"] = 70
                    state["status"] = "downloading" # Still downloading audio
                else:
                    state["progress"] = 90
                    state["status"] = "processing" # Merging/converting
                    
            progress_store[download_id] = state
        return hook

    try:
        # Initialize progress store entry
        progress_store[req.download_id] = {"status": "starting", "progress": 0}

        if req.format == "mp3":
            ydl_opts = {
                "format": "bestaudio/best",
                "outtmpl": tmp_path + ".%(ext)s",
                "postprocessors": [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": req.quality,
                }],
                "quiet": True,
                "no_warnings": True,
                "concurrent_fragment_downloads": 5,
                "nocheckcertificate": True,
                "extractor_args": {
                    "youtube": {
                        "player_client": ["android", "ios"],
                    }
                },
                "postprocessor_args": {
                    "ffmpeg": ["-threads", "4", "-preset", "ultrafast"]
                }
            }
            out_file = tmp_path + ".mp3"
            media_type = "audio/mpeg"
            filename_ext = "mp3"
        else:
            # We prioritize downloading compatible MP4 (H264) and M4A (AAC) streams.
            # If compatible streams are found, they are merged instantly (under 1 second) with no transcoding.
            # If not found (e.g. for resolutions above 1080p), we fall back to best video and audio
            # and transcode to high-quality compatible MP4.
            ydl_opts = {
                "format": f"bestvideo[ext=mp4][height<={req.quality}]+bestaudio[ext=m4a]/bestvideo[height<={req.quality}]+bestaudio/best",
                "outtmpl": tmp_path + ".%(ext)s",
                "merge_output_format": "mp4",
                "recode_video": "mp4",
                "quiet": True,
                "no_warnings": True,
                "concurrent_fragment_downloads": 5,
                "nocheckcertificate": True,
                "extractor_args": {
                    "youtube": {
                        "player_client": ["android", "ios"],
                    }
                },
                "postprocessor_args": {
                    "VideoConvertor+ffmpeg": [
                        "-threads", "4",
                        "-c:v", "libx264",
                        "-crf", "20",      # visually lossless compression
                        "-preset", "ultrafast",
                        "-c:a", "aac",
                        "-b:a", "192k"     # high quality audio
                    ]
                }
            }
            out_file = tmp_path + ".mp4"
            media_type = "video/mp4"
            filename_ext = "mp4"
        
        # Attach the progress hook to options
        ydl_opts["progress_hooks"] = [make_progress_hook(req.download_id)]
        
        # Download video to the temporary path
        # Download video to the temporary path in a separate thread so we don't block the event loop
        def run_download():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(req.url, download=True)
                
        info = await asyncio.to_thread(run_download)
        title = info.get("title", "video")
            
        # Update progress to streaming status
        progress_store[req.download_id] = {"status": "streaming", "progress": 100}
            
        # Clean title for content disposition header
        safe_title = "".join(c for c in title if c.isalnum() or c in " -_").strip()
        if not safe_title:
            safe_title = "video"
            
        # Verify the file exists
        if not os.path.exists(out_file):
            # Fallback if yt-dlp downloaded with a different extension (e.g. if merge failed)
            # Find any file starting with our tmp_id
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

        # Read file and stream to client, ensuring we clean up afterwards
        def iterfile():
            try:
                with open(out_file, "rb") as f:
                    while chunk := f.read(65536): # 64KB chunks
                        yield chunk
            except Exception as e:
                logger.error(f"Error during file streaming: {str(e)}")
            finally:
                # Always remove the file when streaming ends or gets cancelled
                if os.path.exists(out_file):
                    try:
                        os.remove(out_file)
                        logger.info(f"Successfully cleaned up temp file: {out_file}")
                    except Exception as ex:
                        logger.error(f"Failed to remove temp file {out_file}: {str(ex)}")
                
                # Always clean up progress store entry when stream terminates
                if req.download_id in progress_store:
                    try:
                        del progress_store[req.download_id]
                    except Exception:
                        pass

        # As an extra safety net, queue cleanup in background tasks too
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
        logger.error(f"Download error: {str(e)}")
        # Clean up progress store entry on failure
        if req.download_id in progress_store:
            try:
                del progress_store[req.download_id]
            except Exception:
                pass
        # If download failed, clean up any partial files
        cleanup_temp_files_by_id(tmp_dir, tmp_id)
        raise HTTPException(status_code=400, detail=f"Download failed: {str(e)}")

def cleanup_temp_file(file_path: str):
    """Clean up helper for background task."""
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            logger.info(f"Background cleanup removed file: {file_path}")
        except Exception as e:
            logger.error(f"Background cleanup failed for {file_path}: {str(e)}")

def cleanup_temp_files_by_id(directory: str, file_id: str):
    """Finds and removes any temporary files matching the uuid prefix in case of errors."""
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
