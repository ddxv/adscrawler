import json
import sys
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

import fuzzy_company_ticker_mapping as generator
import import_company_ticker_mapping as importer


def test_find_candidates_uses_database_ticker_name_without_ids():
    matches = generator.find_candidates(
        [{"company_name": "Snap Inc.", "company_domain": "snap.com"}],
        [
            {
                "ticker": "SNAP",
                "exchange": "NYSE",
                "ticker_name": "Snap Inc. AKA Snapchat",
            }
        ],
        {},
        set(),
    )

    assert matches == [
        {
            "company_name": "Snap Inc.",
            "company_domain": "snap.com",
            "candidates": [
                {
                    "ticker": "SNAP",
                    "exchange": "NYSE",
                    "ticker_name": "Snap Inc. AKA Snapchat",
                }
            ],
        }
    ]


def test_main_writes_review_json_without_database_ids_or_match_words(
    monkeypatch, tmp_path
):
    blocklist_path = tmp_path / "blocklist.json"
    blocklist_path.write_text(json.dumps({"ticker_domains": {}, "company_names": []}))
    output_path = tmp_path / "candidates.json"
    monkeypatch.setattr(
        generator,
        "load_database_data",
        lambda: (
            [{"company_name": "Snap Inc.", "company_domain": "snap.com"}],
            [
                {
                    "ticker": "SNAP",
                    "exchange": "NYSE",
                    "ticker_name": "Snap Inc. AKA Snapchat",
                }
            ],
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fuzzy_company_ticker_mapping.py",
            "--blocklist",
            str(blocklist_path),
            "--output",
            str(output_path),
        ],
    )

    generator.main()

    payload = json.loads(output_path.read_text())
    candidate = payload["companies"][0]
    assert candidate["company_name"] == "Snap Inc."
    assert candidate["candidates"][0]["ticker_name"] == "Snap Inc. AKA Snapchat"
    assert not {
        "company_id",
        "ticker_id",
        "matched_company_words",
    }.intersection(candidate)
    assert not {
        "company_id",
        "ticker_id",
        "matched_company_words",
    }.intersection(candidate["candidates"][0])
    multiple_payload = json.loads(
        (tmp_path / "candidates_multiple_candidates.json").read_text()
    )
    assert multiple_payload["companies"] == []


def test_load_mappings_uses_review_names_and_ignores_legacy_ids(tmp_path):
    path = tmp_path / "review.json"
    path.write_text(
        json.dumps(
            {
                "companies": [
                    {
                        "company_id": 123,
                        "company_name": "Example Inc.",
                        "company_domain": "example.com",
                        "candidates": [
                            {
                                "ticker_id": 456,
                                "ticker": "EX",
                                "exchange": "NYSE",
                            }
                        ],
                    }
                ],
            }
        )
    )

    assert importer.load_mappings([path]) == [
        ("Example Inc.", "example.com", "EX", "NYSE")
    ]


def test_import_mappings_resolves_ids_from_database_records(monkeypatch, tmp_path):
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS adtech"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS public"))
        conn.execute(
            text(
                "CREATE TABLE public.domains (id INTEGER PRIMARY KEY, domain_name TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE adtech.companies (id INTEGER PRIMARY KEY, name TEXT, domain_id INTEGER)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE adtech.stock_exchanges (id INTEGER PRIMARY KEY, exchange_code TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE adtech.tickers (id INTEGER PRIMARY KEY, symbol TEXT, exchange_id INTEGER)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE adtech.company_ticker_mapping (company_id INTEGER, ticker_id INTEGER UNIQUE)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO public.domains VALUES (1, 'example.com'), (2, 'other.com')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO adtech.companies VALUES (10, 'Example Inc.', 1), (20, 'Other Inc.', 2)"
            )
        )
        conn.execute(text("INSERT INTO adtech.stock_exchanges VALUES (3, 'NYSE')"))
        conn.execute(
            text("INSERT INTO adtech.tickers VALUES (30, 'EX', 3), (40, 'OTHER', 3)")
        )
        conn.execute(text("INSERT INTO adtech.company_ticker_mapping VALUES (20, 40)"))
    wrapped = SimpleNamespace(begin=engine.begin, dispose=lambda: None)
    monkeypatch.setattr(
        importer,
        "get_db_connection",
        lambda: SimpleNamespace(engine=wrapped),
    )
    review_path = tmp_path / "review.json"
    review_path.write_text(
        json.dumps(
            {
                "companies": [
                    {
                        "company_name": "Example Inc.",
                        "company_domain": "example.com",
                        "candidates": [{"ticker": "EX", "exchange": "NYSE"}],
                    }
                ],
            }
        )
    )

    mappings = importer.load_mappings([review_path])
    assert importer.insert_mappings(mappings) == (1, 0)
    with engine.connect() as conn:
        assert conn.execute(
            text(
                "SELECT company_id, ticker_id FROM adtech.company_ticker_mapping ORDER BY ticker_id"
            )
        ).all() == [(10, 30), (20, 40)]
    engine.dispose()


@pytest.mark.parametrize(
    "company",
    [
        {
            "company_domain": "example.com",
            "candidates": [{"ticker": "EX", "exchange": "NYSE"}],
        },
        {"company_name": "Example", "company_domain": "example.com", "candidates": []},
    ],
)
def test_load_mappings_rejects_missing_review_fields(tmp_path, company):
    path = tmp_path / "review.json"
    path.write_text(json.dumps({"companies": [company]}))
    with pytest.raises(ValueError):
        importer.load_mappings([path])
