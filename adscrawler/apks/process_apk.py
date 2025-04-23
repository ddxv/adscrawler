import os
import pathlib
import subprocess
import time

import pandas as pd

from adscrawler.apks import manifest, mitm_process_log
from adscrawler.config import (
    ANDROID_SDK,
    APK_PARTIALS_DIR,
    APKS_DIR,
    PACKAGE_DIR,
    XAPKS_DIR,
    get_logger,
)
from adscrawler.connection import PostgresCon
from adscrawler.queries import get_top_ranks_for_unpacking, query_store_id_map

logger = get_logger(__name__)


def process_apks(
    database_connection: PostgresCon, number_of_apps_to_pull: int = 20
) -> None:
    error_count = 0
    store = 1
    apps = get_top_ranks_for_unpacking(
        database_connection=database_connection,
        store=store,
        limit=number_of_apps_to_pull,
    )
    logger.info(f"Start APK processing: {apps.shape=}")
    for _id, row in apps.iterrows():
        if error_count > 5:
            continue
        if error_count > 0:
            time.sleep(error_count * error_count * 10)
        store_id = row.store_id

        try:
            this_error_count, extension = manifest.process_manifest(
                database_connection=database_connection, row=row
            )
            error_count += this_error_count
        except Exception:
            logger.exception(f"Manifest for {store_id} failed")

        try:
            run_waydroid_app(database_connection, extension, row)
        except Exception:
            logger.exception(f"Waydroid API call scraping for {store_id} failed")

        remove_partial_apks(store_id=store_id)


def get_downloaded_apks() -> list[str]:
    apks = []
    apk_path = pathlib.Path(APKS_DIR)
    for apk in apk_path.glob("*.apk"):
        apks.append(apk.stem)
    return apks


def process_apks_for_waydroid(database_connection: PostgresCon) -> None:
    apks = get_downloaded_apks()
    store_id_map = query_store_id_map(
        database_connection=database_connection, store_ids=apks
    )
    store_id_map = store_id_map.rename(columns={"id": "store_app"})
    for _, row in store_id_map.iterrows():
        logger.info(f"Processing {row.store_id}")
        run_waydroid_app(database_connection, extension=".apk", row=row)


def run_waydroid_app(
    database_connection: PostgresCon, extension: str, row: pd.Series
) -> None:
    store_id = row.store_id
    store_app = row.store_app
    apk_path = pathlib.Path(APKS_DIR, f"{store_id}.apk")
    if extension == ".xapk":
        xapk_path = pathlib.Path(XAPKS_DIR, f"{store_id}{extension}")
        os.system(f"java -jar APKEditor.jar m -i {xapk_path.as_posix()}")
        # APKEditor merged APKs must be signed to install
        apk_path = pathlib.Path(APKS_DIR, f"{store_id}.apk")
        merged_apk_path = pathlib.Path(XAPKS_DIR, f"{store_id}_merged.apk")
        os.system(
            f"{ANDROID_SDK}/apksigner sign --ks ~/.android/debug.keystore  --ks-key-alias androiddebugkey   --ks-pass pass:android   --key-pass pass:android   --out {apk_path}  {merged_apk_path}"
        )
        apk_path.exists()

    logger.info("Clearing mitmdump")
    mitm_script = pathlib.Path(PACKAGE_DIR, "adscrawler/apks/mitm_start.sh")
    os.system(f"{mitm_script.as_posix()} -d")

    # os.system("waydroid session stop")

    # This is required to run weston in cronjob environment
    os.environ["XDG_RUNTIME_DIR"] = "/run/user/1000"

    # This will be the socket name for the weston process
    socket_name = "wayland-98"
    os.environ["WAYLAND_DISPLAY"] = socket_name
    weston_process = start_weston(socket_name)

    waydroid_process = start_waydroid()
    try:
        _mitm_process = launch_and_track_app(store_id, apk_path)
    except Exception:
        logger.exception(f"App {store_id} failed to log traffic")
        if weston_process:
            weston_process.terminate()
        if waydroid_process:
            waydroid_process.terminate()
        raise
    finally:
        if weston_process:
            weston_process.terminate()
        if waydroid_process:
            waydroid_process.terminate()

    mdf = mitm_process_log.parse_mitm_log(store_id)
    logger.info(f"MITM log for {store_id} has {mdf.shape[0]} rows")
    if mdf.empty:
        logger.warning(f"MITM log for {store_id} returned empty dataframe")
        return
    mdf = mdf.rename(columns={"timestamp": "crawled_at"})
    mdf["url"] = mdf["url"].str[0:1000]
    mdf = mdf[["url", "host", "crawled_at", "status_code", "tld_url"]].drop_duplicates()
    mdf["store_app"] = store_app
    mdf.to_sql(
        name="store_app_api_calls",
        con=database_connection.engine,
        if_exists="append",
        index=None,
    )
    logger.info(f"Waydroid mitm log for {store_id} saved to db")


def launch_and_track_app(store_id: str, apk_path: pathlib.Path) -> None:
    function_info = f"Waydroid launch and track {store_id=}"
    mitm_script = pathlib.Path(PACKAGE_DIR, "adscrawler/apks/mitm_start.sh")

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

    install_app(store_id, apk_path)
    waydroid_launch_app(store_id)

    logger.info(f"{function_info} waiting for 60 seconds")
    time.sleep(60)
    logger.info(f"{function_info} stopping app & mitmdump")
    os.system(f"{mitm_script.as_posix()} -d")
    os.system(f'sudo waydroid shell am force-stop "{store_id}"')
    os.system(f'waydroid app remove "{store_id}"')


def waydroid_launch_app(store_id: str) -> None:
    os.system(f'waydroid app launch "{store_id}"')

    time.sleep(2)

    # Set timeout parameters
    timeout = 60  # seconds
    start_time = time.time()
    found = False

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
            logger.info(f"{store_id} is now in the foreground")
            break

        # Wait before launching again
        os.system(f'waydroid app launch "{store_id}"')
        time.sleep(2)

    if not found:
        logger.error(f"{store_id} not found in the foreground after {timeout} seconds")
        raise Exception(f"Waydroid failed to launch {store_id}")


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
    logger.info(f"{function_info} installing")
    os.system(f'waydroid app install "{apk_path}"')

    time.sleep(2)

    timeout = 45
    start_time = time.time()
    while (time.time() - start_time) < timeout:
        applist = subprocess.run(
            ["waydroid", "app", "list"], capture_output=True, text=True, check=False
        )
        output = applist.stdout
        if store_id in output:
            logger.info(f"{function_info} installed")
            break
        time.sleep(1)
    else:
        logger.error(f"{function_info} not installed")
        raise Exception(f"Waydroid failed to install {store_id}")


def start_waydroid() -> subprocess.Popen:
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

    logger.info("Waiting for Waydroid to be ready (120 seconds)...")
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

    if not ready:
        if waydroid_process.poll() is not None:
            logger.error("Waydroid process ended without becoming ready")
        else:
            logger.error(
                f"Timed out after {timeout} seconds waiting for Waydroid to be ready"
            )
            waydroid_process.terminate()
    return waydroid_process


def start_weston(socket_name: str) -> subprocess.Popen:
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


def remove_partial_apks(store_id: str) -> None:
    apk_path = pathlib.Path(APK_PARTIALS_DIR, f"{store_id}.apk")
    if apk_path.exists():
        apk_path.unlink(missing_ok=True)
        logger.info(f"{store_id=} deleted partial apk {apk_path.as_posix()}")
    xapk_path = pathlib.Path(APK_PARTIALS_DIR, f"{store_id}.xapk")
    if xapk_path.exists():
        xapk_path.unlink(missing_ok=True)
        logger.info(f"{store_id=} deleted partial xapk {xapk_path.as_posix()}")


if __name__ == "__main__":
    from adscrawler.connection import get_db_connection

    use_tunnel = False
    database_connection = get_db_connection(use_ssh_tunnel=use_tunnel)
    database_connection.set_engine()
    process_apks_for_waydroid(database_connection=database_connection)
