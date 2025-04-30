import datetime
import os
import pathlib
import subprocess
import time

import pandas as pd

from adscrawler.apks import mitm_process_log
from adscrawler.apks.weston import restart_weston, start_weston
from adscrawler.config import (
    ANDROID_SDK,
    APKS_DIR,
    PACKAGE_DIR,
    WAYDROID_INTERNAL_EMULATED_DIR,
    WAYDROID_MEDIA_DIR,
    XAPKS_DIR,
    XAPKS_TMP_UNZIP_DIR,
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


def extract_and_sign_xapk(store_id: str) -> None:
    """Extract and sign an xapk to creat apk file.
    This method works but requires dependencies on Android SDK for apksigner and APKEditor

    Additionally, it requires using a (debug) keystore.

    Currently not using this method.
    """
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
        launch_and_track_app(store_id, apk_path)
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
    database_connection: PostgresCon,
    store_id: str,
    store_app: int,
    extension: str,
) -> None:
    if extension == "apk":
        apk_path = pathlib.Path(APKS_DIR, f"{store_id}.apk")
    elif extension == "xapk":
        apk_path = pathlib.Path(XAPKS_DIR, f"{store_id}.xapk")
    else:
        raise ValueError(f"Invalid extension: {extension}")
    if not check_container():
        stop_container()
    if not check_session():
        waydroid_process = restart_session()
        if not waydroid_process:
            stop_container()
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
        logger.exception(f"Waydroid failed to run store_id={store_id}")


def check_container() -> bool:
    waydroid_process = subprocess.run(
        ["waydroid", "status"], capture_output=True, text=True, check=False
    )
    # Look specifically for the line "Container:  RUNNING"
    is_container_running = False
    for line in waydroid_process.stdout.splitlines():
        if line.strip() == "Container:\tRUNNING":
            is_container_running = True
    if not is_container_running:
        logger.error("Waydroid container is not running")
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
    app_list = subprocess.run(
        ["waydroid", "app", "list"], capture_output=True, text=True, check=False
    )
    if "waydroid session is stopped" in app_list.stderr.lower():
        is_session_running = False
    return is_session_running


def stop_container() -> None:
    function_info = "Waydroid container"
    logger.info(f"{function_info} stopping")
    os.system("sudo waydroid container stop")
    time.sleep(1)


def restart_session() -> subprocess.Popen | None:
    logger.info("Waydroid session restart")
    os.system("waydroid session stop")

    if not check_wayland_display():
        _weston_process = start_weston()
        if not check_wayland_display():
            restart_weston()
            if not check_wayland_display():
                logger.error("Weston already running but waydroid display not found")
                raise Exception("Weston already running but waydroid display not found")

    waydroid_process = start_session()
    if not waydroid_process:
        logger.error("Waydroid failed to start")
        raise Exception("Waydroid failed to start")
    return waydroid_process


def check_wayland_display() -> bool:
    display = os.environ.get("WAYLAND_DISPLAY")
    xdg_dir = os.environ.get("XDG_RUNTIME_DIR")
    display_is_set = display is not None
    xdg_dir_is_set = xdg_dir is not None
    if display_is_set and xdg_dir_is_set:
        msg = f"Weston check OK: XDG_RUNTIME_DIR:{xdg_dir} WAYLAND_DISPLAY:{display}"
        logger.info(msg)
    else:
        msg = f"Weston check NOK: XDG_RUNTIME_DIR:{xdg_dir} WAYLAND_DISPLAY:{display}"
        logger.error(msg)
    return display_is_set and xdg_dir_is_set

    msg = f"Weston check: XDG_RUNTIME_DIR:{xdg_dir} WAYLAND_DISPLAY:{display}"
    logger.info(msg)
    return


def cleanup_xapk_splits(store_id: str) -> None:
    apk_split_dir = pathlib.Path(WAYDROID_MEDIA_DIR, store_id)
    tmp_apk_dir = pathlib.Path(XAPKS_TMP_UNZIP_DIR, f"{store_id}")
    try:
        if apk_split_dir.is_dir():
            subprocess.run(
                ["sudo", "rm", "-rf", apk_split_dir.as_posix()],
                text=True,
                check=True,
            )
    except Exception:
        logger.exception(f"Exception occurred while cleaning up {apk_split_dir}")
    try:
        if tmp_apk_dir.is_dir():
            subprocess.run(
                ["sudo", "rm", "-rf", tmp_apk_dir.as_posix()],
                text=True,
                check=True,
            )
    except Exception:
        logger.exception(f"Exception occurred while cleaning up {tmp_apk_dir}")


def prep_xapk_splits(store_id: str, xapk_path: pathlib.Path) -> str:
    logger.info(f"Waydroid prep xapk splits for {store_id}")
    tmp_apk_dir = pathlib.Path(XAPKS_TMP_UNZIP_DIR, f"{store_id}")
    if not tmp_apk_dir.exists():
        os.makedirs(tmp_apk_dir)

    unzip_command = f"unzip -o {xapk_path.as_posix()} -d {tmp_apk_dir.as_posix()}"
    _unzip_result = os.system(unzip_command)
    if _unzip_result != 0:
        logger.error(f"Failed to unzip {xapk_path.as_posix()}")
        raise Exception(f"Failed to unzip {xapk_path.as_posix()}")

    list_of_apks = list(tmp_apk_dir.glob("*.apk"))

    list_of_split_apks = [x.name for x in list_of_apks if x.name != f"{store_id}.apk"]
    base_apk_names = [x.name for x in list_of_apks if x.name == f"{store_id}.apk"]
    apk_split_dir = pathlib.Path(WAYDROID_INTERNAL_EMULATED_DIR, store_id)
    if len(base_apk_names) == 1:
        base_apk_name = base_apk_names[0]
        base_apk_path = pathlib.Path(apk_split_dir, base_apk_name)
    else:
        raise ValueError(f"Found {len(base_apk_names)} base apks for {store_id}")

    cp_command = f"sudo cp -r {tmp_apk_dir.as_posix()} {WAYDROID_MEDIA_DIR.as_posix()}"
    _cp_result = os.system(cp_command)
    if _cp_result != 0:
        err = f"Failed to copy {tmp_apk_dir.as_posix()} to {WAYDROID_MEDIA_DIR.as_posix()}"
        logger.error(err)
        raise Exception(err)

    split_apk_paths = [
        pathlib.Path(apk_split_dir, x).as_posix()
        for x in list_of_split_apks
        if x != base_apk_name
    ]

    split_install_command = (
        f"pm install {base_apk_path.as_posix()} {' '.join(split_apk_paths)}"
    )
    return split_install_command


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
        cleanup_xapk_splits(store_id)
        raise

    logger.info(f"{function_info} waiting for 60 seconds")
    time.sleep(60)
    logger.info(f"{function_info} stopping app & mitmdump")
    os.system(f"{mitm_script.as_posix()} -d")
    os.system(f'sudo waydroid shell am force-stop "{store_id}"')
    os.system(f'waydroid app remove "{store_id}"')
    cleanup_xapk_splits(store_id)
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

    if "waydroid session is stopped" in applist.stderr.lower():
        logger.error(f"{function_info} Waydroid session is stopped")
        raise Exception(f"{function_info} Waydroid session is stopped")

    if store_id in applist.stdout:
        logger.info(f"{function_info} found already installed")
        return
    logger.info(f"{function_info} installing {apk_path.as_posix()}")

    time.sleep(2)
    extension = apk_path.suffix
    if extension == ".xapk":
        split_install_command = prep_xapk_splits(store_id, apk_path)
        _install_output = subprocess.run(
            ["sudo", "waydroid", "shell"] + split_install_command.split(),
            capture_output=True,
            text=True,
            check=False,
        )
    elif extension == ".apk":
        _install_output = subprocess.run(
            ["waydroid", "app", "install", apk_path.as_posix()],
            capture_output=True,
            text=True,
            check=False,
        )
    else:
        raise ValueError(f"Invalid extension: {extension}")
    time.sleep(2)

    timeout = 45
    start_time = time.time()
    while (time.time() - start_time) < timeout:
        applist = subprocess.run(
            ["waydroid", "app", "list"], capture_output=True, text=True, check=False
        )
        if store_id in applist.stdout:
            logger.info(f"{function_info} installed")
            return
        if "waydroid session is stopped" in applist.stderr.lower():
            logger.error(f"{function_info} Waydroid session is stopped")
            raise Exception(f"{function_info} Waydroid session is stopped")
        time.sleep(2)

    applist = subprocess.run(
        ["waydroid", "app", "list"], capture_output=True, text=True, check=False
    )

    if store_id in applist.stdout:
        logger.info(f"{function_info} installed")
        return

    raise Exception(
        f"Waydroid failed to install {store_id}{extension} installerror:{_install_output.stderr}"
    )


def start_session() -> subprocess.Popen:
    function_info = "Waydroid session"
    logger.info(f"{function_info} start")
    # Start the Waydroid session process
    waydroid_process = subprocess.Popen(
        ["dbus-run-session", "--", "waydroid", "session", "start"],
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
        logger.info(f"{function_info} readline: {line}")
        if (
            "Android with user 0 is ready" in line
            or "Session is already running" in line
        ):
            ready = True
            logger.info("Waydroid is ready! Continuing with the script...")
            break
        if display_waiting:
            logger.info(f"{function_info} waiting for session to be ready...")
            display_waiting = False
        if "Unable to autolaunch a dbus-daemon" in line:
            logger.exception(f"{function_info} unable to autolaunch a dbus-daemon")
            restart_weston()
            raise Exception(f"{function_info} unable to autolaunch a dbus-daemon")
        time.sleep(1)

    if not ready:
        if waydroid_process.poll() is not None:
            err = waydroid_process.stderr
            logger.error(f"{function_info} process ended without becoming ready {err}")
            raise Exception(
                f"{function_info} process ended without becoming ready {err}"
            )
        else:
            logger.error(
                f"{function_info} session timed out after {timeout} seconds waiting for session to be ready"
            )
            waydroid_process.terminate()
    logger.info(f"{function_info} success")
    return waydroid_process
