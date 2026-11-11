import csv
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

import import_tickers_csv as importer


def test_load_csv_preserves_symbols_and_aliases(tmp_path):
    path = tmp_path / "tickers.csv"
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["exchange", "aliases", "name", "ticker"])
        writer.writerow([" SZSE ", " first|second ", "Name, Inc.", " 000001 "])
        writer.writerow(["NYSE", " ", "Another", "ABC"])
    assert importer.load_tickers(path) == [
        ("000001", "SZSE", "Name, Inc. AKA first|second"),
        ("ABC", "NYSE", "Another"),
    ]


@pytest.mark.parametrize(
    "content",
    [
        "ticker,name,exchange\nABC,Name,NYSE\n",
        "ticker,name,exchange,aliases\nABC,Name,,alias\n",
        "ticker,name,exchange,aliases\nABC,Name,NYSE\n",
        "ticker,name,exchange,aliases\nABC,Name,NYSE,alias,extra\n",
        "ticker,name,exchange,aliases\nABC,Name,EXCHANGE-CODE-TOO-LONG,alias\n",
    ],
)
def test_load_csv_rejects_invalid_input(tmp_path, content):
    path = tmp_path / "tickers.csv"
    path.write_text(content)
    with pytest.raises(ValueError):
        importer.load_tickers(path)


def make_engine(monkeypatch):
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS adtech"))
        conn.execute(
            text(
                "CREATE TABLE adtech.stock_exchanges (id INTEGER PRIMARY KEY, exchange_code TEXT UNIQUE, exchange_name TEXT NOT NULL)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE adtech.tickers (id INTEGER PRIMARY KEY, symbol TEXT, exchange_id INTEGER, ticker_company_name TEXT, UNIQUE (symbol, exchange_id), CHECK (symbol != 'INVALID'))"
            )
        )
        conn.execute(
            text(
                "INSERT INTO adtech.stock_exchanges VALUES (1, 'Nasdaq', 'Original Nasdaq'), (2, 'NYSE', 'Original NYSE')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO adtech.tickers VALUES (1, 'AAPL', 1, 'Original Apple'), (2, 'AAPL', 2, 'Other company')"
            )
        )
    wrapped = SimpleNamespace(begin=engine.begin, dispose=lambda: None)
    monkeypatch.setattr(
        importer, "get_db_connection", lambda: SimpleNamespace(engine=wrapped)
    )
    return engine


def test_import_preserves_existing_and_is_idempotent(monkeypatch):
    engine = make_engine(monkeypatch)
    records = [
        ("AAPL", "NASDAQ", "Replacement AKA alias"),
        ("MSFT", "NASDAQ", "Microsoft"),
        ("000001", "SZSE", "Ping An AKA bank"),
        ("000001", "SZSE", "Duplicate"),
    ]
    assert importer.insert_tickers(records) == (1, 2)
    assert importer.insert_tickers(records) == (0, 0)
    with engine.connect() as conn:
        exchanges = conn.execute(
            text(
                "SELECT exchange_code, exchange_name FROM adtech.stock_exchanges ORDER BY id"
            )
        ).all()
        tickers = conn.execute(
            text(
                "SELECT symbol, exchange_id, ticker_company_name FROM adtech.tickers ORDER BY id"
            )
        ).all()
    assert exchanges == [
        ("Nasdaq", "Original Nasdaq"),
        ("NYSE", "Original NYSE"),
        ("SZSE", "SZSE"),
    ]
    assert tickers == [
        ("AAPL", 1, "Original Apple"),
        ("AAPL", 2, "Other company"),
        ("MSFT", 1, "Microsoft"),
        ("000001", 3, "Ping An AKA bank"),
    ]
    engine.dispose()


def test_failure_rolls_back_new_exchanges_and_tickers(monkeypatch):
    engine = make_engine(monkeypatch)
    with pytest.raises(IntegrityError):
        importer.insert_tickers(
            [("VALID", "NEW", "Valid"), ("INVALID", "NEW", "Invalid")]
        )
    with engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM adtech.stock_exchanges")
            ).scalar_one()
            == 2
        )
        assert (
            conn.execute(text("SELECT COUNT(*) FROM adtech.tickers")).scalar_one() == 2
        )
    engine.dispose()


def test_ambiguous_case_insensitive_exchange_rolls_back(monkeypatch):
    engine = make_engine(monkeypatch)
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO adtech.stock_exchanges VALUES (3, 'NASDAQ', 'NASDAQ')")
        )
    with pytest.raises(ValueError, match="Ambiguous"):
        importer.insert_tickers([("ABC", "nasdaq", "Name")])
    engine.dispose()


def test_repo_csv_loads_if_available():
    path = Path(__file__).resolve().parents[1] / "tickers.csv"
    if not path.exists():
        pytest.skip("Local ticker CSV is not present")
    records = importer.load_tickers(path)
    assert records
    assert ("000001", "SZSE", "Ping An Bank Co Ltd AKA ping an bank") in records
