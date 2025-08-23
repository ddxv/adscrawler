import datetime
import os
import pathlib
import re
import select
import subprocess
import time

import numpy as np
import pandas as pd

from adscrawler.config import (
    ANDROID_SDK,
    APKS_DIR,
    MITM_DIR,
    PACKAGE_DIR,
    WAYDROID_INTERNAL_EMULATED_DIR,
    WAYDROID_MEDIA_DIR,
    XAPKS_DIR,
    XAPKS_TMP_UNZIP_DIR,
    get_logger,
)
from adscrawler.dbcon.connection import PostgresCon
from adscrawler.dbcon.queries import (
    get_version_code_dbid,
    insert_df,
    log_version_code_scan_crawl_results,
    query_apps_to_api_scan,
    query_store_app_by_store_id,
)
from adscrawler.packages.apks import mitm_process_log
from adscrawler.packages.apks.weston import restart_weston, start_weston
from adscrawler.packages.storage import download_to_local, upload_mitm_log_to_s3
from adscrawler.packages.utils import (
    get_local_file_path,
    get_md5_hash,
    get_version,
    remove_tmp_files,
    unzip_apk,
)

logger = get_logger(__name__, "waydroid")

ANDROID_PERMISSION_ACTIVITY = (
    "com.android.permissioncontroller/.permission.ui.ReviewPermissionsActivity"
)


def insert_api_calls(
    version_code_id: int,
    store_id: str,
    version_str: str,
    database_connection: PostgresCon,
    crawl_result: int,
    run_name: str,
    mdf: pd.DataFrame,
) -> int:
    logger.info(f"insert_crawled_at {crawl_result=}")
    insert_columns = ["version_code_id", "run_name", "run_result", "run_at"]
    df = pd.DataFrame(
        {
            "version_code_id": [version_code_id],
            "run_name": [run_name],
            "run_result": [crawl_result],
            "run_at": datetime.datetime.now(tz=datetime.UTC),
        }
    )
    df = df[insert_columns]
    run_df = insert_df(
        df=df,
        table_name="version_code_api_scan_results",
        database_connection=database_connection,
        return_rows=True,
    )
    if not mdf.empty:
        run_id = run_df["id"].to_numpy()[0]
        mdf["run_id"] = run_id
        insert_columns = [
            "store_app",
            "tld_url",
            "url",
            "host",
            "status_code",
            "called_at",
            "run_id",
            "country_id",
            "state_iso",
            "city_name",
            "org",
        ]
        mdf = mdf[insert_columns]
        mdf["country_id"] = np.where(
            np.isnan(mdf["country_id"]), None, mdf["country_id"]
        )
        insert_df(
            df=mdf,
            table_name="store_app_api_calls",
            database_connection=database_connection,
            insert_columns=insert_columns,
        )
        upload_mitm_log_to_s3(
            store=1,
            store_id=store_id,
            version_str=version_str,
            run_id=run_id,
        )
        mitm_process_log.move_mitm_to_processed(store_id, run_id)

        logger.info(f"inserted {mdf.shape[0]} api calls")


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
    run_name: str,
    timeout: int = 60,
) -> None:
    function_info = f"run_app {store_id=}"
    crawl_result = 3
    version_code_id = None
    version_str = None
    logger.info(f"{function_info} clearing mitmdump")
    mitm_script = pathlib.Path(PACKAGE_DIR, "adscrawler/packages/apks/mitm_start.sh")
    os.system(f"{mitm_script.as_posix()} -d")
    mdf = pd.DataFrame()

    try:
        version_str, version_code_id = launch_and_track_app(
            store_id,
            store_app,
            database_connection,
            apk_path,
            timeout=timeout,
        )
        try:
            mdf = process_mitm_log(store_id, store_app, database_connection)
            crawl_result = 1
        except Exception as e:
            crawl_result = 3
            logger.exception(f"{function_info} mitm log ingestion failed: {e}")
    except Exception:
        crawl_result = 2
        logger.exception(f"{function_info} launch_and_track_app failed")
    if version_code_id is None:
        try:
            version_str, version_code_id = get_version_via_apktool(
                store_id, apk_path, store_app, database_connection
            )
        except Exception:
            logger.exception(f"{function_info} apktool failed to get version code")
            version_code_id = None
        if version_code_id is None:
            logger.error(f"{function_info} failed to get version code id")

    md5_hash = get_md5_hash(apk_path)
    logger.info(f"{function_info} log: {md5_hash=} {version_code_id=} {crawl_result=}")
    log_version_code_scan_crawl_results(
        store_app=store_app,
        version_code_id=version_code_id,
        md5_hash=md5_hash,
        crawl_result=crawl_result,
        database_connection=database_connection,
    )

    if version_code_id is None:
        logger.error(
            f"{function_info} failed: No version code id with {mdf.shape[0]} api calls"
        )
        return
    if version_str is None:
        logger.error(
            f"{function_info} failed: No version str with {mdf.shape[0]} api calls"
        )
        return

    insert_api_calls(
        version_code_id=version_code_id,
        store_id=store_id,
        version_str=version_str,
        database_connection=database_connection,
        crawl_result=crawl_result,
        run_name=run_name,
        mdf=mdf,
    )

    logger.info(f"{function_info} success: {mdf.shape[0]} api calls saved to db")


def get_version_via_apktool(
    store_id: str,
    apk_path: pathlib.Path,
    store_app: int,
    database_connection: PostgresCon,
) -> tuple[str, int | None]:
    apk_tmp_decoded_output_path = unzip_apk(store_id, apk_path)
    apktool_info_path = pathlib.Path(apk_tmp_decoded_output_path, "apktool.yml")
    version_str = get_version(apktool_info_path)
    version_code_id = get_version_code_dbid(store_app, version_str, database_connection)
    return version_str, version_code_id


def process_mitm_log(
    store_id: str,
    store_app: int,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    function_info = f"MITM {store_id=}"
    mdf = mitm_process_log.parse_mitm_log(store_id, database_connection)
    logger.info(f"{function_info} log has {mdf.shape[0]} rows")
    if mdf.empty:
        logger.warning(f"{function_info} MITM log returned empty dataframe")
    else:
        mdf = mdf.rename(columns={"timestamp": "called_at"})
        mdf["url"] = mdf["url"].str[0:1000]
        mdf = mdf[
            [
                "url",
                "host",
                "called_at",
                "status_code",
                "tld_url",
                "country_id",
                "state_iso",
                "city_name",
                "org",
            ]
        ].drop_duplicates()
        mdf["store_app"] = store_app
    return mdf


def process_app_for_waydroid(
    database_connection: PostgresCon,
    store_id: str,
    store_app: int,
    apk_path: pathlib.Path,
    run_name: str,
    timeout: int = 60,
) -> None:
    if not apk_path.exists():
        raise FileNotFoundError(f"{apk_path=} not found")
    if not check_container() or not check_session():
        waydroid_process = restart_session()
        if waydroid_process:
            logger.info(f"Waydroid restarted with pid: {waydroid_process.pid}")
        else:
            kill_waydroid()
            waydroid_process = restart_session()
            if not waydroid_process:
                logger.error("Waydroid failed to start")
                return
            logger.info(f"Waydroid restarted with pid: {waydroid_process.pid}")

    try:
        run_app(
            database_connection,
            apk_path=apk_path,
            store_id=store_id,
            store_app=store_app,
            run_name=run_name,
            timeout=timeout,
        )
    except Exception:
        logger.exception(f"Waydroid failed to run store_id={store_id}")


def check_container() -> bool:
    waydroid_process = subprocess.run(
        ["waydroid", "status"],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
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
        ["waydroid", "status"],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    # Look specifically for the line "Session:  RUNNING"
    is_session_running = False
    for line in waydroid_process.stdout.splitlines():
        logger.info(line.strip())
        if "Session" in line and "RUNNING" in line:
            is_session_running = True
    app_list = subprocess.run(
        ["waydroid", "app", "list"],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    if "waydroid session is stopped" in app_list.stderr.lower():
        err = app_list.stderr
        logger.error(
            f"Check session: waydroid app list returned session is stopped {err}"
        )
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
    time.sleep(1)
    if not check_session():
        logger.error("Waydroid failed check session")
        raise Exception("Waydroid failed check session")
    return waydroid_process


def check_wayland_display() -> bool:
    display = os.environ.get("WAYLAND_DISPLAY")
    xdg_dir = os.environ.get("XDG_RUNTIME_DIR")
    display_is_set = display is not None
    xdg_dir_is_set = xdg_dir is not None
    msg = f"XDG_RUNTIME_DIR:{xdg_dir} WAYLAND_DISPLAY:{display}"
    if display_is_set and xdg_dir_is_set:
        msg = f"Weston check OK: {msg}"
        logger.info(msg)
    else:
        msg = f"Weston check NOK: {msg}"
        logger.error(msg)
    return display_is_set and xdg_dir_is_set


def cleanup_waydroid_apk_files(store_id: str) -> None:
    """Cleanup waydroid apk files for a given store_id.
    APKs are copied direclty to the media dir.
    XAPKS have an additional unzip step in the tmp folder.
    """
    for path in [WAYDROID_MEDIA_DIR, XAPKS_TMP_UNZIP_DIR]:
        app_path = pathlib.Path(path, store_id)
        try:
            subprocess.run(
                ["sudo", "rm", "-rf", app_path.as_posix()],
                text=True,
                check=True,
                timeout=60,
            )
        except Exception:
            logger.exception(f"Exception occurred while cleaning up {app_path}")


def remove_all_third_party_apps() -> None:
    function_info = "Remove all third party apps"
    logger.info(f"{function_info} start")
    third_party_apps = subprocess.run(
        ["sudo", "waydroid", "shell", "pm", "list", "packages", "-3"],
        text=True,
        capture_output=True,
        check=True,
        timeout=20,
    )
    apps_to_remove = [
        x.replace("package:", "") for x in third_party_apps.stdout.splitlines()
    ]
    apps_to_remove = [x for x in apps_to_remove if x not in THIRD_PARTY_APPS_TO_KEEP]
    for app in apps_to_remove:
        logger.info(f"{function_info} removing '{app}'")
        subprocess.run(
            ["sudo", "waydroid", "shell", "pm", "uninstall", app],
            text=True,
            check=True,
            timeout=30,
        )
    logger.info(f"{function_info} success")


THIRD_PARTY_APPS_TO_KEEP = ["org.mozilla.firefox", "io.github.huskydg.magisk"]


def prep_xapk_splits(store_id: str, xapk_path: pathlib.Path) -> str:
    logger.info(f"Waydroid prep xapk splits for {store_id}")
    tmp_apk_dir = pathlib.Path(XAPKS_TMP_UNZIP_DIR, f"{store_id}")
    if not tmp_apk_dir.exists():
        os.makedirs(tmp_apk_dir)

    unzip_command = f"unzip -o {xapk_path.as_posix()} -d {tmp_apk_dir.as_posix()}"
    _unzip_result = os.system(unzip_command)
    if _unzip_result != 0:
        err = f"Failed to unzip {unzip_command} with err:{_unzip_result}"
        logger.error(err)
        raise Exception(err)

    list_of_apks = list(tmp_apk_dir.glob("*.apk"))

    list_of_split_apks = [x.name for x in list_of_apks if x.name != f"{store_id}.apk"]
    base_apk_names = [x.name for x in list_of_apks if x.name == f"{store_id}.apk"]
    apk_split_dir = pathlib.Path(WAYDROID_INTERNAL_EMULATED_DIR, store_id)
    if len(base_apk_names) == 1:
        base_apk_name = base_apk_names[0]
        base_apk_path = pathlib.Path(apk_split_dir, base_apk_name)
    else:
        base_apk_names = [x.name for x in list_of_apks if x.name == "base.apk"]
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


def get_installed_version_str(
    store_id: str, store_app: int, database_connection: PostgresCon
) -> tuple[str | None, int | None]:
    package_info = subprocess.run(
        ["sudo", "waydroid", "shell", "dumpsys", "package", store_id],
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    version_info = package_info.stdout

    match = re.search(r"versionCode=(\d+)", version_info)
    version_code_id = None
    version_str = None
    if match:
        version_str = match.group(1)
        logger.info(f"found versionCode: {version_str}")
        version_code_id = get_version_code_dbid(
            store_app, version_str, database_connection
        )

    if version_code_id is None:
        logger.error(f"No version code id found for {store_id=}")
    return version_str, version_code_id


def launch_and_track_app(
    store_id: str,
    store_app: int,
    database_connection: PostgresCon,
    apk_path: pathlib.Path,
    timeout: int = 60,
) -> tuple[str, int]:
    function_info = f"waydroid {store_id=} launch and track"
    mitm_script = pathlib.Path(PACKAGE_DIR, "adscrawler/packages/apks/mitm_start.sh")

    install_app(store_id, apk_path)

    logger.info(
        f"{function_info} starting mitmdump with script {mitm_script.as_posix()}"
    )
    mitm_logfile = pathlib.Path(MITM_DIR, f"traffic_{store_id}.log")
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
        remove_app(store_id)
        raise

    logger.info(f"{function_info} waiting for {timeout} seconds")
    time.sleep(timeout)
    logger.info(f"{function_info} stopping app & mitmdump")
    os.system(f"{mitm_script.as_posix()} -d")
    version_str, version_code_id = get_installed_version_str(
        store_id, store_app, database_connection
    )
    remove_app(store_id)
    if version_code_id is None:
        raise Exception(f"{function_info} failed to get version code")
    logger.info(f"{function_info} {version_code_id=} success")
    return version_str, version_code_id


def remove_app(store_id: str) -> None:
    function_info = f"waydroid {store_id=} remove files"
    logger.info(f"{function_info} start")
    try:
        os.system(f'sudo waydroid shell am force-stop "{store_id}"')
        os.system(f'waydroid app remove "{store_id}"')
        cleanup_waydroid_apk_files(store_id)
        logger.info(f"{function_info} success")
    except Exception as e:
        logger.exception(f"{function_info} failed: {e}")
        raise


def kill_waydroid() -> None:
    function_info = "Waydroid kill"
    logger.info(f"{function_info} start")
    stop_container()
    time.sleep(1)
    os.system("waydroid session stop")
    time.sleep(1)
    os.system("sudo pkill waydroid")
    logger.info(f"{function_info} success")


def launch_app(store_id: str) -> None:
    function_info = f"waydroid {store_id=} launch"
    logger.info(f"{function_info} start")
    os.system(f"waydroid app launch {store_id}")

    time.sleep(2)

    # Set timeout parameters
    timeout = 60
    start_time = time.time()
    found = False
    permission_attempts = 2
    last_relaunch_time = time.time()

    # Loop until timeout or app is found
    while time.time() - start_time < timeout and not found:
        # Run the waydroid shell dumpsys command
        result = subprocess.run(
            ["sudo", "waydroid", "shell", "dumpsys", "activity", "activities"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        time.sleep(1)

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

        logger.info(f"{function_info} not in foreground yet")
        # Relaunch every 10 seconds
        if time.time() - last_relaunch_time >= 20:
            logger.info(f"{function_info} relaunching")
            os.system(f"waydroid app launch {store_id}")
            last_relaunch_time = time.time()
        # Wait before checking again
        time.sleep(1)

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
        err = applist.stderr
        logger.error(f"{function_info} Waydroid session is stopped: {err}")
        check_session()
        raise Exception(f"{function_info} Waydroid session is stopped: {err}")

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
            timeout=120,
        )
    elif extension == ".apk":
        _install_output = subprocess.run(
            ["waydroid", "app", "install", apk_path.as_posix()],
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
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

    logger.info(f"{function_info} start loop")
    while (
        waydroid_process.poll() is None
        and not ready
        and (time.time() - start_time) < timeout
    ):
        rlist, _, _ = select.select(
            [waydroid_process.stdout], [], [], 1.0
        )  # 1-second timeout for select

        logger.info(f"{function_info} rlist: {rlist}")

        if rlist:
            line = waydroid_process.stdout.readline()
            logger.info(f"{function_info} line: {line=}")
            if line:
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
                    logger.exception(
                        f"{function_info} unable to autolaunch a dbus-daemon"
                    )
                    raise Exception(
                        f"{function_info} unable to autolaunch a dbus-daemon"
                    )
                if "container is not running" in line:
                    logger.error(f"{function_info} container is not running")
                    raise Exception(f"{function_info} container failed to start")
        time.sleep(1)

    if not ready:
        if waydroid_process.poll() is not None:
            stdout = waydroid_process.stdout.read() if waydroid_process.stdout else ""
            stderr = waydroid_process.stderr.read() if waydroid_process.stderr else ""
            msg = f"{function_info} process ended without becoming ready stdout:{stdout} stderr:{stderr}"
            raise Exception(msg)
        else:
            logger.error(
                f"{function_info} session timed out after {timeout} seconds waiting for session to be ready"
            )
            waydroid_process.terminate()
    logger.info(f"{function_info} success")
    return waydroid_process


def manual_waydroid_process(
    database_connection: PostgresCon,
    store_id: str,
    timeout: int,
    run_name: str,
) -> None:
    logger.info(f"Manual waydroid process for {store_id=}")
    store = 1
    download_from_s3 = False
    try:
        apk_path = get_local_file_path(store, store_id)
        if apk_path is None:
            download_from_s3 = True
    except FileNotFoundError:
        download_from_s3 = True
    if download_from_s3:
        apk_path, _version_str = download_to_local(store=store, store_id=store_id)
    if not apk_path or not apk_path.exists():
        raise FileNotFoundError(f"{store_id=} not found")

    store_app = query_store_app_by_store_id(database_connection, store_id)
    process_app_for_waydroid(
        database_connection=database_connection,
        store_id=store_id,
        store_app=store_app,
        apk_path=apk_path,
        timeout=timeout,
        run_name=run_name,
    )
    remove_all_third_party_apps()


def process_apks_for_waydroid(
    database_connection: PostgresCon, num_apps: int = 20
) -> None:
    apps_df = query_apps_to_api_scan(database_connection=database_connection, store=1)
    logger.info(f"Waydroid has {apps_df.shape[0]} apps to process, starting {num_apps}")
    apps_df = apps_df.head(num_apps)
    for _, row in apps_df.iterrows():
        logger.info(f"Start app {_}/{apps_df.shape[0]}: {row.store_id}")
        store_id = row.store_id
        store_app = row.store_app
        version_str = row.version_string
        try:
            apk_path, _version_str = download_to_local(
                store=1, store_id=store_id, version_str=version_str
            )
            if not apk_path:
                raise FileNotFoundError(f"APK file not found for {store_id=}")
        except FileNotFoundError:
            logger.error(f"Waydroid failed to download {store_id}")
            continue
        process_app_for_waydroid(
            database_connection=database_connection,
            apk_path=apk_path,
            store_id=store_id,
            store_app=store_app,
            run_name="regular",
        )
        remove_tmp_files(store_id=store_id)
    remove_all_third_party_apps()
