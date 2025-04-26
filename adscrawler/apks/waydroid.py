import datetime
import os
import pathlib
import subprocess
import time

import pandas as pd

from adscrawler.apks import mitm_process_log
from adscrawler.config import (
    ANDROID_SDK,
    APKS_DIR,
    PACKAGE_DIR,
    XAPKS_DIR,
    get_logger,
)
from adscrawler.connection import PostgresCon
from adscrawler.queries import upsert_df

logger = get_logger(__name__)

ANDROID_PERMISSION_ACTIVITY = (
    "com.android.permissioncontroller/.permission.ui.ReviewPermissionsActivity"
)


def upsert_crawled_at(
    store_app: int, database_connection: PostgresCon, crawl_result: int
) -> None:
    logger.info(f"upsert_crawled_at {store_app=} {crawl_result=}")
    key_columns = ["store_app", "crawl_result", "crawled_at"]
    upsert_df(
        table_name="store_app_waydroid_crawled_at",
        schema="logging",
        insert_columns=["store_app", "crawl_result", "crawled_at"],
        df=pd.DataFrame(
            {
                "store_app": [store_app],
                "crawl_result": [crawl_result],
                "crawled_at": datetime.datetime.now(tz=datetime.UTC),
            }
        ),
        key_columns=key_columns,
        database_connection=database_connection,
    )


def extract_and_sign_xapk(store_id: str) -> pathlib.Path:
    xapk_path = pathlib.Path(XAPKS_DIR, f"{store_id}.xapk")
    os.system(f"java -jar APKEditor.jar m -i {xapk_path.as_posix()}")
    # APKEditor merged APKs must be signed to install
    apk_path = pathlib.Path(APKS_DIR, f"{store_id}.apk")
    merged_apk_path = pathlib.Path(XAPKS_DIR, f"{store_id}_merged.apk")
    os.system(
        f"{ANDROID_SDK}/apksigner sign --ks ~/.android/debug.keystore  --ks-key-alias androiddebugkey   --ks-pass pass:android   --key-pass pass:android   --out {apk_path}  {merged_apk_path}"
    )


def run_app(
    database_connection: PostgresCon,
    apk_path: pathlib.Path,
    store_id: str,
    store_app: int,
) -> None:
    function_info = f"waydroid {store_id=}"
    crawl_result = 3

    logger.info(f"{function_info} clearing mitmdump")
    mitm_script = pathlib.Path(PACKAGE_DIR, "adscrawler/apks/mitm_start.sh")
    os.system(f"{mitm_script.as_posix()} -d")

    try:
        _mitm_process = launch_and_track_app(store_id, apk_path)
        try:
            process_mitm_log(store_id, database_connection, store_app)
            crawl_result = 1
        except Exception as e:
            crawl_result = 3
            logger.exception(f"{function_info} failed: {e}")
    except Exception as e:
        crawl_result = 2
        logger.exception(f"{function_info} failed: {e}")
    logger.info(f"{function_info} saved to db")
    upsert_crawled_at(store_app, database_connection, crawl_result)


def process_mitm_log(
    store_id: str, database_connection: PostgresCon, store_app: int
) -> None:
    function_info = f"MITM {store_id=}"
    mdf = mitm_process_log.parse_mitm_log(store_id)
    logger.info(f"{function_info} log has {mdf.shape[0]} rows")
    if mdf.empty:
        logger.warning(f"{function_info} MITM log returned empty dataframe")
    else:
        mdf = mdf.rename(columns={"timestamp": "crawled_at"})
        mdf["url"] = mdf["url"].str[0:1000]
        mdf = mdf[
            ["url", "host", "crawled_at", "status_code", "tld_url"]
        ].drop_duplicates()
        mdf["store_app"] = store_app
        mdf.to_sql(
            name="store_app_api_calls",
            con=database_connection.engine,
            if_exists="append",
            index=None,
        )


def process_app_for_waydroid(
    database_connection: PostgresCon, apk_path: str, store_id: str, store_app: int
) -> None:
    if not check_container():
        restart_container()
    if not check_session():
        waydroid_process = restart_session()
        if not waydroid_process:
            logger.error("Waydroid failed to start")
            return
    try:
        run_app(
            database_connection,
            apk_path=apk_path,
            store_id=store_id,
            store_app=store_app,
        )
    except Exception:
        try:
            restart_session()
        except Exception:
            logger.exception("Failed to restart waydroid, exiting")
            return


def check_container() -> bool:
    waydroid_process = subprocess.run(
        ["waydroid", "status"], capture_output=True, text=True, check=False
    )
    # Look specifically for the line "Session:  RUNNING"
    is_container_running = False
    for line in waydroid_process.stdout.splitlines():
        print(line.strip())
        if line.strip() == "Container:\tRUNNING":
            is_container_running = True
    return is_container_running


def check_session() -> bool:
    waydroid_process = subprocess.run(
        ["waydroid", "status"], capture_output=True, text=True, check=False
    )
    # Look specifically for the line "Session:  RUNNING"
    is_session_running = False
    for line in waydroid_process.stdout.splitlines():
        print(line.strip())
        if line.strip() == "Session:\tRUNNING":
            is_session_running = True
    return is_session_running


def restart_container() -> None:
    function_info = "Waydroid container"
    logger.info(f"{function_info} restarting")
    os.system("sudo waydroid container stop")
    time.sleep(1)
    restart_session()


def restart_session() -> subprocess.Popen | None:
    os.system("waydroid session stop")

    # This is required to run weston in cronjob environment
    os.environ["XDG_RUNTIME_DIR"] = "/run/user/1000"

    if not check_wayland_display():
        _weston_process = start_weston()

    waydroid_process = start_session()
    if not waydroid_process:
        logger.error("Waydroid failed to start")
        return
    return waydroid_process


def check_wayland_display() -> bool:
    return os.environ.get("WAYLAND_DISPLAY") is not None


def launch_and_track_app(store_id: str, apk_path: pathlib.Path) -> None:
    function_info = f"waydroid {store_id=} launch and track"
    mitm_script = pathlib.Path(PACKAGE_DIR, "adscrawler/apks/mitm_start.sh")

    install_app(store_id, apk_path)
    logger.info(
        f"{function_info} starting mitmdump with script {mitm_script.as_posix()}"
    )
    mitm_logfile = pathlib.Path(PACKAGE_DIR, f"mitmlogs/traffic_{store_id}.log")
    mitm_process = subprocess.Popen(
        [f"{mitm_script.as_posix()}", "-w", "-s", mitm_logfile.as_posix()],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    # Store the PID
    mitm_pid = mitm_process.pid
    logger.info(f"{function_info} mitmdump started with PID: {mitm_pid}")

    try:
        launch_app(store_id)
    except Exception as e:
        logger.exception(f"{function_info} failed: {e}")
        os.system(f'sudo waydroid shell am force-stop "{store_id}"')
        os.system(f'waydroid app remove "{store_id}"')
        raise

    logger.info(f"{function_info} waiting for 60 seconds")
    time.sleep(60)
    logger.info(f"{function_info} stopping app & mitmdump")
    os.system(f"{mitm_script.as_posix()} -d")
    os.system(f'sudo waydroid shell am force-stop "{store_id}"')
    os.system(f'waydroid app remove "{store_id}"')
    logger.info(f"{function_info} success")


def launch_app(store_id: str) -> None:
    function_info = f"waydroid {store_id=} launch"
    logger.info(f"{function_info} start")
    os.system(f'waydroid app launch "{store_id}"')

    time.sleep(2)

    # Set timeout parameters
    timeout = 60  # seconds
    start_time = time.time()
    found = False
    permission_attempts = 2

    # Loop until timeout or app is found
    while time.time() - start_time < timeout and not found:
        # Run the waydroid shell dumpsys command
        result = subprocess.run(
            ["sudo", "waydroid", "shell", "dumpsys", "activity", "activities"],
            capture_output=True,
            text=True,
            check=False,
        )

        # Check if app is in the output
        if store_id in result.stdout:
            found = True
            logger.info(f"{function_info} in foreground")
            break

        if ANDROID_PERMISSION_ACTIVITY in result.stdout:
            logger.warning(f"{function_info} PERMISSIONS REQUEST IN FOREGROUND")
            permission_attempts -= 1
            if permission_attempts <= 0:
                raise Exception(f"{function_info} PERMISSIONS REQUEST IN FOREGROUND")

        # Wait before launching again
        logger.info(f"{function_info} not in foreground, relaunching")
        os.system(f'waydroid app launch "{store_id}"')
        time.sleep(2)

    if not found:
        logger.error(
            f"{function_info} not found in the foreground after {timeout} seconds"
        )
        raise Exception(f"waydroid {store_id=} failed to launch")
    logger.info(f"{function_info} success")


def install_app(store_id: str, apk_path: pathlib.Path) -> None:
    function_info = f"Waydroid install {store_id=}"
    logger.info(f"{function_info} checking")
    applist = subprocess.run(
        ["waydroid", "app", "list"], capture_output=True, text=True, check=False
    )

    output = applist.stdout

    if store_id in output:
        logger.info(f"{function_info} found already installed")
        return
    logger.info(f"{function_info} installing {apk_path.as_posix()}")

    _install_output = subprocess.run(
        ["waydroid", "app", "install", apk_path.as_posix()],
        capture_output=True,
        text=True,
        check=False,
    )

    time.sleep(2)

    timeout = 30
    start_time = time.time()
    while (time.time() - start_time) < timeout:
        applist = subprocess.run(
            ["waydroid", "app", "list"], capture_output=True, text=True, check=False
        )
        output = applist.stdout
        if store_id in output:
            logger.info(f"{function_info} installed")
            return
        time.sleep(1)

    applist = subprocess.run(
        ["waydroid", "app", "list"], capture_output=True, text=True, check=False
    )

    output = applist.stdout

    if store_id in output:
        logger.info(f"{function_info} installed")
        return

    raise Exception(f"Waydroid failed to install {store_id}")


def start_session() -> subprocess.Popen:
    function_info = "Waydroid session"
    logger.info(f"{function_info} start")
    # Start the Waydroid session process
    waydroid_process = subprocess.Popen(
        ["waydroid", "session", "start"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    # Set a timeout (in seconds)
    timeout = 120  # Wait up to 2 minutes
    start_time = time.time()
    ready = False
    display_waiting = True

    while (
        waydroid_process.poll() is None
        and not ready
        and (time.time() - start_time) < timeout
    ):
        line = waydroid_process.stdout.readline()
        logger.info(line)
        if (
            "Android with user 0 is ready" in line
            or "Session is already running" in line
        ):
            ready = True
            logger.info("Waydroid is ready! Continuing with the script...")
            break
        if display_waiting:
            logger.info(f"{function_info} waiting for ready (120 seconds)...")
            display_waiting = False
        time.sleep(1)

    if not ready:
        if waydroid_process.poll() is not None:
            logger.error("Waydroid process ended without becoming ready")
        else:
            logger.error(
                f"Timed out after {timeout} seconds waiting for Waydroid to be ready"
            )
            waydroid_process.terminate()
    logger.info(f"{function_info} success")
    return waydroid_process


def start_weston() -> subprocess.Popen:
    # This will be the socket name for the weston process
    socket_name = "wayland-98"
    os.environ["WAYLAND_DISPLAY"] = socket_name

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
            logger.info("Weston is ready! Continuing with the script...")
            break

    if not ready:
        if weston_process.poll() is not None:
            logger.error("Weston process ended without becoming ready")
        else:
            logger.error(
                f"Timed out after {timeout} seconds waiting for Waydroid to be ready"
            )
            weston_process.terminate()
        return
    return weston_process
