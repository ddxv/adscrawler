import hashlib
import pathlib
import subprocess
import uuid

import imagehash
import pandas as pd
from PIL import Image

from adscrawler.config import CREATIVES_DIR, get_logger

logger = get_logger(__name__, "mitm_scrape_ads")


def extract_frame_at(local_path: pathlib.Path, second: int) -> Image.Image:
    """Extracts a single frame from a video file at the specified second."""
    tmp_path = pathlib.Path(f"/tmp/frame_{uuid.uuid4()}.jpg")
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-ss",
            str(second),
            "-i",
            str(local_path),
            "-vframes",
            "1",
            "-q:v",
            "2",
            "-f",
            "image2",
            str(tmp_path),
        ],
        check=True,
    )
    img = Image.open(tmp_path)
    return img


def average_hashes(hashes: list[imagehash.ImageHash]) -> str:
    """Computes the average hash from multiple image hashes using majority voting."""
    # imagehash returns a numpy-like object; sum and majority voting works
    bits = sum([h.hash.astype(int) for h in hashes])
    majority = (bits >= (len(hashes) / 2)).astype(int)
    return str(imagehash.ImageHash(majority))


def compute_phash_multiple_frames(local_path: pathlib.Path, seconds: list[int]) -> str:
    """Computes perceptual hash from multiple video frames at specified time points."""
    hashes = []
    for second in seconds:
        try:
            hashes.append(imagehash.phash(extract_frame_at(local_path, second)))
        except Exception:
            pass
    phash = average_hashes(hashes)
    return str(phash)


def get_phash(md5_hash: str, adv_store_id: str, file_extension: str) -> str:
    """Generates a perceptual hash for a creative file, using multiple frames for videos."""
    phash = None
    local_path = (
        pathlib.Path(CREATIVES_DIR, adv_store_id) / f"{md5_hash}.{file_extension}"
    )
    seekable_formats = {"mp4", "webm", "gif"}
    if file_extension in seekable_formats:
        try:
            seconds = [1, 3, 5, 10]
            phash = str(compute_phash_multiple_frames(local_path, seconds))
        except Exception:
            logger.error("Failed to compute multiframe phash")
    if phash is None:
        phash = str(imagehash.phash(Image.open(local_path)))
    return phash


def store_creatives(row: pd.Series, adv_store_id: str, file_extension: str) -> str:
    """Stores creative files locally and generates thumbnails, returning the MD5 hash."""
    thumbnail_width = 320
    local_dir = pathlib.Path(CREATIVES_DIR, adv_store_id)
    local_dir.mkdir(parents=True, exist_ok=True)
    thumbs_dir = CREATIVES_DIR / "thumbs"
    thumbs_dir.mkdir(parents=True, exist_ok=True)
    md5_hash = hashlib.md5(row["response_content"]).hexdigest()
    local_path = local_dir / f"{md5_hash}.{file_extension}"
    with open(local_path, "wb") as creative_file:
        creative_file.write(row["response_content"])
    thumb_path = thumbs_dir / f"{md5_hash}.jpg"
    # Only generate thumbnail if not already present
    seekable_formats = {"mp4", "webm", "gif"}
    static_formats = {"jpg", "jpeg", "png", "webp"}
    if not thumb_path.exists():
        try:
            ext = file_extension.lower()
            if ext in seekable_formats:
                try:
                    # Attempt to extract a thumbnail at 5s (works for video or animated gif/webp)
                    subprocess.run(
                        [
                            "ffmpeg",
                            "-y",
                            "-ss",
                            "5",
                            "-i",
                            str(local_path),
                            "-vframes",
                            "1",
                            "-vf",
                            f"scale={thumbnail_width}:-1",
                            "-q:v",
                            "2",
                            "-update",
                            "1",
                            str(thumb_path),
                        ],
                        check=True,
                        stdout=subprocess.DEVNULL,
                    )
                except subprocess.CalledProcessError:
                    # Fallback: use first frame (or static image frame)
                    subprocess.run(
                        [
                            "ffmpeg",
                            "-y",
                            "-i",
                            str(local_path),
                            "-vframes",
                            "1",
                            "-vf",
                            f"scale={thumbnail_width}:-1",
                            "-q:v",
                            "2",
                            "-update",
                            "1",
                            str(thumb_path),
                        ],
                        check=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
            elif ext in static_formats:
                # Static images: no need to seek, just resize
                subprocess.run(
                    [
                        "ffmpeg",
                        "-y",
                        "-i",
                        str(local_path),
                        "-vf",
                        f"scale={thumbnail_width}:-1",
                        "-q:v",
                        "2",
                        "-update",
                        "1",
                        str(thumb_path),
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                )
            else:
                logger.error(f"Unknown file extension: {file_extension} for thumbnail!")
        except Exception:
            logger.error(f"Failed to create thumbnail for {local_path}")
            pass
    return md5_hash
