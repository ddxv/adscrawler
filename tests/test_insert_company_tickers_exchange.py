import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

import insert_company_tickers_exchange as importer


def test_load_tickers_uses_named_fields_and_skips_missing_exchange(tmp_path):
    path = tmp_path / "tickers.json"
    path.write_text(
        json.dumps(
            {
                "fields": ["exchange", "name", "ticker"],
                "data": [
                    [" Nasdaq ", " Apple Inc. ", " AAPL "],
                    [None, "Other", "OTHR"],
                ],
            }
        )
    )

    assert importer.load_tickers(path) == ([("AAPL", "Nasdaq", "Apple Inc.")], 1)


@pytest.mark.parametrize(
    "fields,data",
    [
        (["ticker", "exchange"], [["AAPL", "Nasdaq"]]),
        (["ticker", "exchange", "name"], [["AAPL", "Nasdaq"]]),
        (["ticker", "exchange", "name"], [["AAPL", "Nasdaq", " "]]),
    ],
)
def test_load_tickers_rejects_missing_names(tmp_path, fields, data):
    path = tmp_path / "tickers.json"
    path.write_text(json.dumps({"fields": fields, "data": data}))
    with pytest.raises(ValueError):
        importer.load_tickers(path)


def test_import_inserts_and_updates_only_matching_tickers(monkeypatch):
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS adtech"))
        conn.execute(
            text(
                "CREATE TABLE adtech.stock_exchanges (id INTEGER PRIMARY KEY, exchange_code TEXT UNIQUE, exchange_name TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE adtech.tickers (id INTEGER PRIMARY KEY, symbol TEXT, exchange_id INTEGER, ticker_company_name TEXT, UNIQUE (symbol, exchange_id))"
            )
        )
        conn.execute(
            text(
                "INSERT INTO adtech.stock_exchanges VALUES (1, 'Nasdaq', 'Nasdaq'), (2, 'NYSE', 'NYSE')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO adtech.tickers VALUES (1, 'AAPL', 1, NULL), (2, 'AAPL', 2, 'Other company'), (3, 'MSFT', 1, 'Old name')"
            )
        )

    wrapped_engine = SimpleNamespace(begin=engine.begin, dispose=lambda: None)
    monkeypatch.setattr(
        importer, "get_db_connection", lambda: SimpleNamespace(engine=wrapped_engine)
    )
    records = [
        ("AAPL", "Nasdaq", "Apple Inc."),
        ("MSFT", "Nasdaq", "Microsoft Corp"),
        ("NVDA", "Nasdaq", "NVIDIA Corp"),
    ]

    assert importer.insert_tickers(records) == (0, 1, 2)
    assert importer.insert_tickers(records) == (0, 0, 0)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, symbol, exchange_id, ticker_company_name FROM adtech.tickers ORDER BY id"
            )
        ).all()
    assert rows == [
        (1, "AAPL", 1, "Apple Inc."),
        (2, "AAPL", 2, "Other company"),
        (3, "MSFT", 1, "Microsoft Corp"),
        (4, "NVDA", 1, "NVIDIA Corp"),
    ]
    engine.dispose()
