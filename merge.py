import re

with open("old_proxy_main.py", "r") as f:
    old_code = f.read()

with open("main.py", "r") as f:
    current_code = f.read()

# Extract RouteManager and dependencies from old_proxy_main.py
deps = """
import base64
import httpx
import random

IS_ON_CLOUD = os.environ.get("SPACE_ID") is not None or os.environ.get("RENDER") is not None

def b64_decode_str(s):
    return base64.b64decode(s).decode("utf-8")
"""

route_manager = re.search(r'(class RouteManager:.*?router_pool = RouteManager\(\))', old_code, re.DOTALL).group(1)

# Add it to current main.py
new_code = current_code.replace(
    "# Initialize rate limiter",
    deps + "\n" + route_manager + "\n\n# Initialize rate limiter"
)

# Modify get_video_info in new_code to use proxy
info_proxy_logic = """
        # Try up to 3 different verified nodes
        info = None
        last_error = None
        for attempt in range(3):
            node = await router_pool.get_active_node() if IS_ON_CLOUD else None
            if not node and IS_ON_CLOUD:
                if attempt > 0:
                    break
                raise HTTPException(
                    status_code=503,
                    detail="All download nodes are currently busy or offline. Please retry in a few seconds."
                )
            
            opts = dict(ydl_opts)
            if node:
                opts["proxy"] = node
            
            logger.info(f"Attempt {attempt + 1}: Fetching info (node={node}, cookies={HAS_COOKIES})")
            
            def run_info():
                with yt_dlp.YoutubeDL(opts) as ydl:
                    return ydl.extract_info(req.url, download=False)
            
            try:
                info = await asyncio.to_thread(run_info)
                if info:
                    break
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Attempt {attempt + 1} failed: {last_error}")
                if node and node in router_pool.nodes:
                    try:
                        router_pool.nodes.remove(node)
                    except Exception:
                        pass
        
        if not info:
            raise Exception(f"Failed to fetch metadata. Last error: {last_error}")
"""
new_code = re.sub(
    r'logger\.info\(f"Fetching info for: \{req\.url\} \(cookies=\{HAS_COOKIES\}\)"\).*?if not info:\s+raise Exception\("yt-dlp returned no data"\)',
    info_proxy_logic.strip(),
    new_code,
    flags=re.DOTALL
)

# Modify download_video in new_code to use proxy
download_proxy_logic = """
        info = None
        last_error = None
        for attempt in range(2):
            node = await router_pool.get_active_node() if IS_ON_CLOUD else None
            if not node and IS_ON_CLOUD:
                if attempt > 0:
                    break
                raise HTTPException(
                    status_code=503,
                    detail="All download nodes are currently busy or offline. Please retry in a few seconds."
                )
            
            opts = dict(ydl_opts)
            if node:
                opts["proxy"] = node
                
            logger.info(f"Download attempt {attempt + 1}: node={node}")
            
            def run_download():
                with yt_dlp.YoutubeDL(opts) as ydl:
                    return ydl.extract_info(req.url, download=True)
            
            try:
                info = await asyncio.to_thread(run_download)
                if info:
                    break
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Download attempt {attempt + 1} failed: {last_error}")
                cleanup_temp_files_by_id(tmp_dir, tmp_id)
                if node and node in router_pool.nodes:
                    try:
                        router_pool.nodes.remove(node)
                    except Exception:
                        pass
        else:
            raise Exception(f"Download failed after multiple attempts: {last_error}")
"""
new_code = re.sub(
    r'logger\.info\(f"Starting download: \{req\.url\}.*?if not info:\s+raise Exception\("Download returned no data"\)',
    download_proxy_logic.strip(),
    new_code,
    flags=re.DOTALL
)

with open("main.py", "w") as f:
    f.write(new_code)
print("Merged proxy logic into main.py")
