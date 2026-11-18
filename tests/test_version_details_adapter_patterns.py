from unittest.mock import Mock

import duckdb

from adscrawler.process import version_details as vd


def test_adapter_matches_coexist_with_other_sdk_patterns(tmp_path, monkeypatch):
    con = duckdb.connect()

    def parquet(key, query):
        path = tmp_path / key
        path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY ({query}) TO '{path}' (FORMAT PARQUET)")

    parquet(
        vd.LOOKUP_VERSION_STRINGS,
        """SELECT * FROM (VALUES
            (1, 'COM.NET.MEDIATION.ADAPTERS.FACEBOOK', NULL),
            (2, 'com.net.mediation.adapters.facebook.SomeClass', NULL),
            (3, 'com.net.mediation.adapters.facebookextra.Class', NULL),
            (4, NULL, 'test/path'),
            (5, 'com.net.mediation.adapters.unityextra.Class', NULL),
            (6, 'com.net.mediation.adapters.Unity.Class', NULL)
        ) AS t(id, value_name, xml_path)""",
    )
    parquet(
        vd.LOOKUP_SDK_PACKAGE_PATTERNS,
        "SELECT * FROM (VALUES (10, 'com.net'), (20, 'com.net.mediation.adapters.facebook')) AS t(sdk_id, package_pattern)",
    )
    parquet(
        vd.LOOKUP_SDK_PATH_PATTERNS,
        "SELECT * FROM (VALUES (30, 'test/path')) AS t(sdk_id, path_pattern)",
    )
    parquet(
        vd.LOOKUP_SDK_MEDIATION_PATTERNS,
        "SELECT * FROM (VALUES (10, 'com.net.mediation.adapters')) AS t(sdk_id, mediation_pattern)",
    )
    parquet(
        vd.LOOKUP_SDK_MEDIATION_ADAPTER_PATTERNS,
        "SELECT * FROM (VALUES (10, 20, 'com.net.mediation.adapters.facebook'), (10, 21, 'com.net.mediation.adapters.unity')) AS t(mediation_sdk_id, adapter_sdk_id, adapter_pattern)",
    )

    class LocalConnection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, query):
            return con.execute(query.replace("s3://test-bucket/", f"{tmp_path}/"))

    monkeypatch.setattr(vd, "CONFIG", {"s3": {"bucket": "test-bucket"}})
    monkeypatch.setattr(vd, "get_duckdb_connection", lambda _: LocalConnection())
    monkeypatch.setattr(vd, "delete_s3_objects_by_prefix", lambda *args: None)
    monkeypatch.setattr(
        vd,
        "get_parquet_paths_by_prefix",
        lambda bucket, prefix: [str(p) for p in (tmp_path / prefix).rglob("*.parquet")],
    )
    swap = Mock()
    monkeypatch.setattr(vd, "replace_s3_prefix", swap)
    (tmp_path / vd.TMP_PATTERN_MATCHES).parent.mkdir(parents=True, exist_ok=True)

    vd.build_aggregated_pattern_matches()

    files = list((tmp_path / vd.TMP_PATTERN_MATCHES).rglob("*.parquet"))
    rows = con.execute(
        "SELECT string_id, sdk_id FROM read_parquet(?)", [[str(p) for p in files]]
    ).fetchall()
    assert set(rows) == {
        (1, 10),
        (1, 20),
        (2, 10),
        (2, 20),
        (3, 10),
        (3, 20),
        (4, 30),
        (5, 10),
        (6, 10),
        (6, 21),
    }
    assert len(rows) == len(set(rows))
    swap.assert_called_once_with(
        "test-bucket", vd.TMP_PATTERN_MATCHES, vd.AGG_PATTERN_MATCHES
    )
    con.close()


def test_copy_lookups_exports_adapter_mapping(monkeypatch):
    export = Mock()
    monkeypatch.setattr(vd, "_pg_table_to_s3", export)

    vd.copy_lookups()

    export.assert_any_call(
        "SELECT mediation_sdk_id, adapter_sdk_id, adapter_pattern FROM pg.adtech.sdk_mediation_adapter_patterns ORDER BY adapter_sdk_id, adapter_pattern",
        vd.LOOKUP_SDK_MEDIATION_ADAPTER_PATTERNS,
        "sdk_mediation_adapter_patterns",
    )
