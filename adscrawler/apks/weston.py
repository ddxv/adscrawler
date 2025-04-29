import os
import subprocess
import time

from adscrawler.config import get_logger

logger = get_logger(__name__)


def start_weston() -> subprocess.Popen:
    logger.info("Starting Weston")
    # This will be the socket name for the weston process
    socket_name = "wayland-98"
    os.environ["WAYLAND_DISPLAY"] = socket_name
    # This is required to run weston in cronjob environment
    os.environ["XDG_RUNTIME_DIR"] = "/run/user/1000"

    weston_process = subprocess.Popen(
        [
            "weston",
            "-B",
            "headless",
            "--width=800",
            "--height=800",
            "--scale=1",
            "-S",
            socket_name,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    ready = False
    timeout = 30
    start_time = time.time()
    while (
        weston_process.poll() is None
        and not ready
        and (time.time() - start_time) < timeout
    ):
        line = weston_process.stdout.readline()
        logger.info(line)
        if "launching '/usr/libexec/weston-desktop-shell'" in line:
            ready = True
            break
        if "maybe another compositor is running" in line:
            logger.info("Weston already running")
            ready = True
            break

    if not ready:
        if weston_process.poll() is not None:
            logger.error("Weston process ended without becoming ready")
        else:
            logger.error(
                f"Weston timed out after {timeout} seconds waiting to be ready"
            )
            weston_process.terminate()
        raise Exception("Weston failed to start")
    logger.info("Weston is ready! Returning process")
    return weston_process
