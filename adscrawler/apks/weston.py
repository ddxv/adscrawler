import os
import pathlib
import subprocess
import time

from adscrawler.config import get_logger

logger = get_logger(__name__)


WESTON_SOCKET_NAME = "wayland-98"


def restart_weston() -> None:
    if is_weston_running():
        logger.info("Weston already running, killing...")
        kill_weston()
    start_weston()


def kill_weston() -> None:
    subprocess.run(["sudo", "pkill", "weston"], check=False)


def is_weston_running() -> bool:
    try:
        result = subprocess.run(
            ["pgrep", "-af", f"weston.*-S {WESTON_SOCKET_NAME}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
        )
        return result.returncode == 0
    except Exception as e:
        logger.warning(f"Failed to check if weston is running: {e}")
        return False


def start_weston() -> subprocess.Popen:
    logger.info("Starting Weston")
    os.environ["WAYLAND_DISPLAY"] = WESTON_SOCKET_NAME
    xdg_runtime_dir = "/tmp/xdg-runtime"
    if not pathlib.Path(xdg_runtime_dir).is_dir():
        os.makedirs(xdg_runtime_dir, exist_ok=True)
        os.chmod(xdg_runtime_dir, 0o700)

    os.environ["XDG_RUNTIME_DIR"] = xdg_runtime_dir

    if is_weston_running():
        logger.info(f"Weston already running with socket '{WESTON_SOCKET_NAME}'")
        return

    weston_process = subprocess.Popen(
        [
            "weston",
            "-B",
            "headless",
            "--width=800",
            "--height=800",
            "--scale=1",
            "-S",
            WESTON_SOCKET_NAME,
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
        logger.info(line.strip())
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
