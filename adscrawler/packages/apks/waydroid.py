import datetime
import os
import pathlib
import subprocess
import time

import numpy as np
import pandas as pd

from adscrawler.config import (
    MITM_DIR,
    PACKAGE_DIR,
    WAYDROID_INTERNAL_EMULATED_DIR,
    WAYDROID_MEDIA_DIR,
    XAPKS_TMP_UNZIP_DIR,
    get_logger,
)
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import (
    get_version_code_dbid,
    get_version_codes_for_store_id,
    insert_df,
    log_version_code_scan_crawl_results,
    query_all_domains,
    query_apps_mitm_in_s3,
    query_apps_to_api_scan,
    query_store_app_by_store_id,
    upsert_df,
)
from adscrawler.metrics import WAYDROID_RUN_RESULTS_COUNTER
from adscrawler.mitm_ad_parser import mitm_logs
from adscrawler.mitm_ad_parser.mitm_logs import parse_log
from adscrawler.packages.apks.weston import (
    is_weston_running,
    restart_weston,
    start_weston,
)
from adscrawler.packages.utils import (
    get_local_file_path,
    get_md5_hash,
    get_version,
    remove_tmp_files,
    unzip_apk,
)
from adscrawler.process.storage import (
    download_app_to_local,
    set_iptables_rule_for_wt0,
    upload_mitm_log_to_s3,
)

logger = get_logger(__name__, "waydroid")

_waydroid_process: subprocess.Popen | None = None
WAYDROID_CONTAINER_FD_LIMIT = 800

ANDROID_PERMISSION_ACTIVITY = (
    "com.android.permissioncontroller/.permission.ui.ReviewPermissionsActivity"
)


def run_app(
    pgdb: PostgresEngine,
    apk_path: pathlib.Path,
    store_id: str,
    store_app: int,
    run_name: str,
    version_code_id: int,
    version_str: str,
    timeout: int = 60,
) -> None:
    function_info = f"run_app {store_id=}"
    crawl_result = 3
    logger.info(f"{function_info} clearing mitmdump")
    mitm_script = pathlib.Path(PACKAGE_DIR, "adscrawler/packages/apks/mitm_start.sh")
    os.system(f"{mitm_script.as_posix()} -d")
    mdf = pd.DataFrame()
    try:
        launch_and_track_app(
            store_id,
            apk_path,
            timeout=timeout,
        )
        crawl_result = 1
    except Exception:
        crawl_result = 2
        logger.exception(f"{function_info} launch_and_track_app failed")

    md5_hash = get_md5_hash(apk_path)
    logger.info(f"{function_info} log: {md5_hash=} {version_code_id=} {crawl_result=}")
    # Log to logging table
    log_version_code_scan_crawl_results(
        store_app=store_app,
        version_code_id=version_code_id,
        md5_hash=md5_hash,
        crawl_result=crawl_result,
        pgdb=pgdb,
    )
    WAYDROID_RUN_RESULTS_COUNTER.add(
        1,
        attributes={
            "run_name": str(run_name),
            "run_result": str(crawl_result),
        },
    )

    try:
        mdf = mitm_logs.parse_log(store_id=store_id, run_id=None, pgdb=pgdb)
        if mdf.empty:
            logger.warning("MITM log is empty")
        else:
            logger.info(f"MITM log has {mdf.shape[0]:,} rows")
            mdf["url"] = mdf["url"].str[0:1000]
            mdf["store_app"] = store_app
    except Exception:
        logger.exception(f"{function_info} process_mitm_log failed")
        mdf = pd.DataFrame()
        crawl_result = 2

    logger.info(f"insert version_code_api_scan_results {crawl_result=}")
    crawl_df = pd.DataFrame(
        {
            "version_code_id": [version_code_id],
            "run_name": [run_name],
            "run_result": [crawl_result],
            "run_at": datetime.datetime.now(tz=datetime.UTC),
        }
    )
    # Main table recording if the api scan was successful, generates run_id
    run_df = insert_df(
        df=crawl_df,
        table_name="version_code_api_scan_results",
        pgdb=pgdb,
        return_rows=True,
    )

    if not mdf.empty:
        run_id = run_df["id"].to_numpy()[0]
        record_mitm_to_db(
            run_id=run_id,
            mdf=mdf,
            pgdb=pgdb,
        )
        upload_mitm_log_to_s3(
            store=1,
            store_id=store_id,
            version_str=version_str,
            run_id=run_id,
        )


def manual_reprocess_mitm(
    run_id: int,
    store_id: str,
    store_app: int,
    pgdb: PostgresEngine,
) -> None:
    apps_df = query_apps_mitm_in_s3(pgdb=pgdb)
    rows = apps_df.shape[0]
    store_id_missing_mitm_logs = []
    # ndf = apps_df[_i:].copy()
    for _i, app in apps_df.iterrows():
        logger.info(f"Processing {_i}/{rows} {app['store_id']}")
        store_id = app["store_id"]
        store_app = app["store_app"]
        run_id = app["run_id"]
        try:
            mdf = parse_log(
                store_id=store_id,
                run_id=run_id,
                pgdb=pgdb,
            )
        except FileNotFoundError:
            logger.error(f"MITM log not found for {store_id=} {run_id=}")
            store_id_missing_mitm_logs.append(store_id)
            continue
        if mdf.empty:
            logger.warning("MITM log is empty")
            continue
        else:
            mdf["url"] = mdf["url"].str[0:1000]
            mdf["store_app"] = store_app
        record_mitm_to_db(
            run_id=run_id,
            mdf=mdf,
            pgdb=pgdb,
        )


def record_mitm_to_db(
    run_id: int,
    mdf: pd.DataFrame,
    pgdb: PostgresEngine,
) -> None:
    mdf["run_id"] = run_id
    # mdf['mitm_uuid'] = mdf['mitm_uuid'].str[:-2] + '11'
    gdf = mitm_logs.make_ip_geo_snapshot_df(
        mdf[["mitm_uuid", "ip_address"]].copy(), pgdb
    )
    gdf["country_id"] = np.where(np.isnan(gdf["country_id"]), None, gdf["country_id"])
    # WARNING: this shouldn't be used outside of tests, try insert instead
    cols = ["mitm_uuid", "ip_address", "country_id", "state_iso", "city_name", "org"]
    logger.info("Upsert geo df")
    gdf = upsert_df(
        df=gdf[
            ["mitm_uuid", "ip_address", "country_id", "state_iso", "city_name", "org"]
        ],
        table_name="ip_geo_snapshots",
        key_columns=["mitm_uuid"],
        insert_columns=cols,
        pgdb=pgdb,
        return_rows=True,
    ).rename(columns={"id": "ip_geo_snapshot_id"})
    gdf["mitm_uuid"] = gdf["mitm_uuid"].astype(str)
    mdf = pd.merge(
        mdf,
        gdf[["ip_geo_snapshot_id", "mitm_uuid"]],
        left_on="mitm_uuid",
        right_on="mitm_uuid",
        how="left",
        validate="1:1",
    )
    logger.info("Insert API calls")
    insert_api_calls(
        pgdb=pgdb,
        mdf=mdf,
    )


def insert_api_calls(
    pgdb: PostgresEngine,
    mdf: pd.DataFrame,
) -> int:
    insert_columns = [
        "run_id",
        "store_app",
        "mitm_uuid",
        "flow_type",
        "tld_url",
        "status_code",
        "request_mime_type",
        "response_mime_type",
        "response_size_bytes",
        "url",
        "ip_geo_snapshot_id",
        "called_at",
    ]
    mdf = mdf[insert_columns]
    insert_df(
        df=mdf,
        table_name="api_calls",
        pgdb=pgdb,
        insert_columns=insert_columns,
    )
    try:
        insert_missing_ad_domains(
            api_calls_df=mdf,
            pgdb=pgdb,
        )
    except Exception:
        logger.exception("Failed to insert missing ad domains")
    logger.info(f"inserted {mdf.shape[0]:,} api calls")


def insert_missing_ad_domains(
    api_calls_df: pd.DataFrame,
    pgdb: PostgresEngine,
) -> None:
    """Adds missing ad domains to the database."""
    logger.info("Checking all ad domains for new ones")
    domains_df = query_all_domains(pgdb=pgdb).rename(columns={"id": "domain_id"})
    check_cols = ["tld_url"]
    for col in check_cols:
        missing_ad_domains = api_calls_df[
            (~api_calls_df[col].isin(domains_df["domain_name"]))
            & (api_calls_df[col].notna())
        ]
        if not missing_ad_domains.empty:
            new_ad_domains = (
                missing_ad_domains[[col]]
                .drop_duplicates()
                .rename(columns={col: "domain_name"})
            )
            upsert_df(
                table_name="domains",
                df=new_ad_domains,
                insert_columns=["domain_name"],
                key_columns=["domain_name"],
                pgdb=pgdb,
            )

    return


def get_version_via_apktool(
    store_id: str,
    apk_path: pathlib.Path,
    store_app: int,
    pgdb: PostgresEngine,
) -> tuple[str, int | None]:
    apk_tmp_decoded_output_path = unzip_apk(store_id, apk_path)
    apktool_info_path = pathlib.Path(apk_tmp_decoded_output_path, "apktool.yml")
    version_str = get_version(apktool_info_path)
    version_code_id = get_version_code_dbid(store_app, version_str, pgdb)
    return version_str, version_code_id


def process_app_for_waydroid(
    pgdb: PostgresEngine,
    store_id: str,
    store_app: int,
    apk_path: pathlib.Path,
    run_name: str,
    version_code_id: int,
    version_str: str,
    timeout: int = 60,
) -> None:
    if not apk_path.exists():
        raise FileNotFoundError(f"{apk_path=} not found")
    fd_count = get_waydroid_container_fd_count()
    if fd_count is not None and fd_count > WAYDROID_CONTAINER_FD_LIMIT:
        logger.warning(
            f"Waydroid container manager has {fd_count} FDs; restarting it"
        )
        restart_waydroid_container()
    if not check_container() or not check_session():
        waydroid_process = restart_session(run_name)
        if waydroid_process:
            logger.info(f"Waydroid restarted with pid: {waydroid_process.pid}")
        else:
            kill_waydroid()
            waydroid_process = restart_session(run_name)
            if not waydroid_process:
                logger.error("Waydroid failed to start")
                return
            logger.info(f"Waydroid restarted with pid: {waydroid_process.pid}")

    try:
        run_app(
            pgdb,
            apk_path=apk_path,
            store_id=store_id,
            store_app=store_app,
            run_name=run_name,
            version_code_id=version_code_id,
            version_str=version_str,
            timeout=timeout,
        )
    except Exception:
        logger.exception(f"Waydroid failed to run store_id={store_id}")


def check_container() -> bool:
    container_service = subprocess.run(
        ["sudo", "systemctl", "status", "waydroid-container.service"],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    if container_service.returncode != 0:
        logger.error("Waydroid container is not running")
    return container_service.returncode == 0


def get_waydroid_container_fd_count() -> int | None:
    result = subprocess.run(
        ["pgrep", "-f", r"/usr/bin/waydroid container start"],
        capture_output=True,
        text=True,
        check=False,
    )
    pids = result.stdout.strip().splitlines()
    if not pids:
        return None

    fd_dir = pathlib.Path("/proc", pids[0], "fd")
    try:
        return sum(1 for _ in fd_dir.iterdir())
    except FileNotFoundError:
        return None


def check_session() -> bool:
    waydroid_process = subprocess.run(
        ["waydroid", "status"],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
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


def start_container(timeout: int = 60) -> None:
    function_info = "Waydroid container"
    logger.info(f"{function_info} starting")
    subprocess.run(
        ["sudo", "systemctl", "stop", "waydroid-container.service"],
        check=False,
        timeout=timeout,
    )
    subprocess.run(
        ["sudo", "systemctl", "start", "waydroid-container.service"],
        check=True,
        timeout=timeout,
    )

    start_time = time.time()
    while time.time() - start_time < timeout:
        if check_container():
            logger.info(f"{function_info} started")
            return
        time.sleep(1)

    raise TimeoutError(f"{function_info} failed to start within {timeout} seconds")


def restart_waydroid_container(timeout: int = 60) -> None:
    function_info = "Waydroid container"
    logger.info(f"{function_info} restart")
    subprocess.run(
        ["sudo", "systemctl", "restart", "waydroid-container.service"],
        check=True,
        timeout=timeout,
    )

    start_time = time.time()
    while time.time() - start_time < timeout:
        if check_container():
            logger.info(f"{function_info} restarted")
            return
        time.sleep(1)

    raise TimeoutError(f"{function_info} failed to restart within {timeout} seconds")


def restart_session(run_name) -> subprocess.Popen | None:
    global _waydroid_process

    logger.info("Waydroid session restart")
    os.system("waydroid session stop")
    if _waydroid_process is not None:
        try:
            _waydroid_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            _waydroid_process.terminate()
            try:
                _waydroid_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                _waydroid_process.kill()
                _waydroid_process.wait()
        finally:
            _waydroid_process = None

    if not check_container():
        start_container()

    if "manual" in run_name:
        pass
    elif is_wayland_env_set() or is_weston_running():
        logger.info("Restarting Weston since env not fully set")
        restart_weston()
        if not is_wayland_env_set():
            msg = "Weston restart failed wayland env not set"
            logger.error(msg)
            raise Exception(msg)
        if not is_weston_running():
            msg = "Weston restart failed weston not running"
            logger.error(msg)
            raise Exception(msg)
    else:
        logger.info("Starting Weston since wayland env not set and weston not running")
        _weston_process = start_weston()

    waydroid_process = start_session()
    if not waydroid_process:
        logger.error("Waydroid failed to start")
        raise Exception("Waydroid failed to start")
    time.sleep(1)
    if not check_session():
        logger.error("Waydroid failed check session")
        raise Exception("Waydroid failed check session")
    _waydroid_process = waydroid_process
    return waydroid_process


def is_wayland_env_set() -> bool:
    display = os.environ.get("WAYLAND_DISPLAY")
    xdg_dir = os.environ.get("XDG_RUNTIME_DIR")
    display_is_set = display is not None
    xdg_dir_is_set = xdg_dir is not None
    msg = f"XDG_RUNTIME_DIR:{xdg_dir} WAYLAND_DISPLAY:{display}"
    if display_is_set and xdg_dir_is_set:
        msg = f"Wayland env check OK: {msg}"
        logger.info(msg)
    else:
        msg = f"Wayland env check NOK: {msg}"
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
    os.system(f"sudo bash -c 'rm -rf {WAYDROID_MEDIA_DIR}/*'")


THIRD_PARTY_APPS_TO_KEEP = ["org.mozilla.firefox", "io.github.huskydg.magisk"]


def prep_xapk_splits(store_id: str, xapk_path: pathlib.Path) -> list[str]:
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

    split_apk_paths = [base_apk_path.as_posix()] + split_apk_paths

    return split_apk_paths


# def get_installed_version_str(
#     store_id: str, store_app: int, pgdb: PostgresEngine
# ) -> tuple[str | None, int]:
#     package_info = subprocess.run(
#         ["sudo", "waydroid", "shell", "dumpsys", "package", store_id],
#         capture_output=True,
#         text=True,
#         check=False,
#         timeout=20,
#     )

#     version_info = package_info.stdout

#     match = re.search(r"versionCode=(\d+)", version_info)
#     version_code_id = None
#     version_str = None
#     if match:
#         version_str = match.group(1)
#         logger.info(f"found versionCode: {version_str}")
#         try:
#             version_code_id = get_version_code_dbid(store_app, version_str, pgdb)
#         except ValueError:
#             logger.error(f"No version code id found for {store_id=}")
#             raise

#     return version_str, version_code_id


def launch_and_track_app(
    store_id: str,
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
        [mitm_script.as_posix(), "-w", "-s", mitm_logfile.as_posix()],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    logger.info(f"{function_info} mitmdump started with PID: {mitm_process.pid}")

    try:
        launch_app(store_id)
        logger.info(f"{function_info} waiting for {timeout} seconds")
        time.sleep(timeout)
    finally:
        logger.info(f"{function_info} stopping app and mitmdump")
        try:
            remove_app(store_id)
        except Exception:
            logger.exception(f"{function_info} failed to remove app")

        # The shell script runs mitmdump in the foreground. Kill the process
        # group so both the script and its mitmdump child are reaped.
        try:
            os.killpg(mitm_process.pid, 15)
        except ProcessLookupError:
            pass
        try:
            mitm_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(mitm_process.pid, 9)
            except ProcessLookupError:
                pass
            mitm_process.wait()
        subprocess.run(
            [mitm_script.as_posix(), "-d"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    # if version_code_id is None:
    # raise Exception(f"{function_info} failed to get version code")
    logger.info(f"{function_info} success")
    # return version_str, version_code_id


def remove_app(store_id: str) -> None:
    function_info = f"waydroid {store_id=} remove files"
    logger.info(f"{function_info} start")
    try:
        os.system(f'sudo waydroid shell am force-stop "{store_id}"')
        subprocess.run(
            ["sudo", "waydroid", "shell", "pm", "uninstall", store_id],
            text=True,
            capture_output=True,
            check=True,
            timeout=20,
        )
        cleanup_waydroid_apk_files(store_id)
        logger.info(f"{function_info} success")
    except Exception as e:
        logger.exception(f"{function_info} failed: {e}")
        raise


def kill_waydroid() -> None:
    function_info = "Waydroid kill"
    logger.info(f"{function_info} start")
    os.system("waydroid session stop")
    stop_container()
    subprocess.run(
        ["sudo", "systemctl", "stop", "waydroid-container.service"],
        check=True,
        timeout=60,
    )
    time.sleep(1)
    os.system("sudo pkill waydroid")
    start_container()
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
        split_apk_paths = prep_xapk_splits(store_id, apk_path)
        _install_output = subprocess.run(
            ["sudo", "waydroid", "shell", "pm", "install"] + split_apk_paths,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
        if (
            "Split null was defined multiple times"
            in _install_output.stdout + _install_output.stderr
        ):
            logger.warning(f"{function_info} retrying without asset splits")
            filtered_paths = [p for p in split_apk_paths if "-asset" not in p]
            _install_output = subprocess.run(
                ["sudo", "waydroid", "shell", "pm", "install"] + filtered_paths,
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
    global _waydroid_process

    function_info = "Waydroid session"
    logger.info(f"{function_info} start")
    # Start the Waydroid session process
    waydroid_process = subprocess.Popen(
        ["waydroid", "session", "start"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )

    # Set a timeout (in seconds)
    timeout = 120  # Wait up to 2 minutes
    start_time = time.time()
    ready = False
    logger.info(f"{function_info} start loop")
    while (
        waydroid_process.poll() is None
        and not ready
        and (time.time() - start_time) < timeout
    ):
        status = subprocess.run(
            ["waydroid", "status"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        status_output = status.stdout.lower()
        if "session:" in status_output and "running" in status_output:
            ready = True
            logger.info("Waydroid is ready! Continuing with the script...")
            break
        time.sleep(1)

    if not ready:
        if waydroid_process.poll() is not None:
            stdout = "redirected to DEVNULL"
            stderr = "redirected to DEVNULL"
            msg = f"{function_info} process ended without becoming ready stdout:{stdout} stderr:{stderr}"
            raise Exception(msg)
        else:
            logger.error(
                f"{function_info} session timed out after {timeout} seconds waiting for session to be ready"
            )
            waydroid_process.terminate()
            try:
                waydroid_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                waydroid_process.kill()
                waydroid_process.wait()
        _waydroid_process = None
        raise Exception(f"{function_info} failed to become ready")
    logger.info(f"{function_info} success")
    _waydroid_process = waydroid_process
    return waydroid_process


def manual_waydroid_process(
    pgdb: PostgresEngine,
    store_id: str,
    timeout: int,
    run_name: str,
    version_code: str | None = None,
) -> None:
    logger.info(f"Manual waydroid process for {store_id=}")
    store = 1
    download_from_s3 = False
    try:
        apk_path = get_local_file_path(store, store_id)
        if apk_path is None:
            vcdf = get_version_codes_for_store_id(pgdb, store_id)
            if version_code:
                vcdf = vcdf[vcdf["version_code_str"] == version_code]
                vcs = vcdf.shape[0]
                if vcs == 0:
                    raise ValueError("No APK found, version code df empty")
                if vcs > 1:
                    logger.warning(
                        f"Found multiple apk hashes for {version_code=} {vcdf=}"
                    )
                    raise ValueError("Multip APKs for single version_code")
            for _i, row in vcdf.iterrows():
                if row.apk_hash:
                    apk_path = get_local_file_path(store, f"{store_id}_{row.apk_hash}")
        if apk_path is None:
            download_from_s3 = True
    except FileNotFoundError:
        download_from_s3 = True
    if download_from_s3:
        apk_path, _version_str = download_app_to_local(
            store=store, store_id=store_id, version_str=version_code
        )
    if not apk_path or not apk_path.exists():
        raise FileNotFoundError(f"{store_id=} not found")

    store_app = query_store_app_by_store_id(pgdb, store_id)
    version_code_id = get_version_code_dbid(store_app, version_code, pgdb)
    process_app_for_waydroid(
        pgdb=pgdb,
        store_id=store_id,
        store_app=store_app,
        apk_path=apk_path,
        timeout=timeout,
        version_str=version_code,
        version_code_id=version_code_id,
        run_name=run_name,
    )
    remove_all_third_party_apps()


def process_apks_for_waydroid(
    pgdb: PostgresEngine, num_apps: int = 20, run_name: str = "regular"
) -> None:
    apps_df = query_apps_to_api_scan(
        pgdb=pgdb, store=1, run_name=run_name, limit=num_apps
    )
    if apps_df.empty:
        if run_name != "regular":
            run_name = "regular"
            apps_df = query_apps_to_api_scan(
                pgdb=pgdb, store=1, run_name=run_name, limit=100
            )
            apps_df = apps_df.tail(num_apps)
            if apps_df.empty:
                logger.info("Waydroid no apps in queue")
    logger.info(f"Waydroid {run_name=} apps={apps_df.shape[0]:,} start")
    set_iptables_rule_for_wt0()
    for _, row in apps_df.iterrows():
        logger.info(
            f"Start app {_}/{apps_df.shape[0]:,}: {row.store_id} version={row.version_string}"
        )
        store_id = row.store_id
        store_app = row.store_app
        version_str = row.version_string
        version_code_id = row.version_code_id
        try:
            apk_path, _version_str = download_app_to_local(
                store=1, store_id=store_id, version_str=version_str
            )
            if not apk_path:
                raise FileNotFoundError(f"APK file not found for {store_id=}")
        except FileNotFoundError:
            logger.error(f"Waydroid failed to download {store_id}")
            continue
        process_app_for_waydroid(
            pgdb=pgdb,
            apk_path=apk_path,
            store_id=store_id,
            store_app=store_app,
            version_code_id=version_code_id,
            version_str=version_str,
            run_name=run_name,
        )
        remove_tmp_files(store_id=store_id)
    remove_all_third_party_apps()
