"""Tests for parse_messages.py"""

import csv
import io
import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(__file__))
from parse_messages import extract_whale_alert, extract_symbol_block, parse_row, main


WHALE_LONG_TEXT = (
    "**Whale Alert:** Hyperliquid Whale **(0x99dd)** ** Long ** **ETH** with **6x** leverage, "
    "entry price **$2188.07**, position value **$3.01M**. \n\n"
    "View details on CoinGlass."
)

WHALE_SHORT_TEXT = (
    "**Whale Alert:** Hyperliquid Whale **(0xe60d)** ** Short ** **ETH** with **15x** leverage, "
    "entry price **$2186.95**, position value **$2.08M**. \n\n"
    "View details on CoinGlass."
)

SYMBOL_BLOCK_TEXT = (
    "`Symbol              BTC\n"
    "Price               $70859.90\n"
    "Market Cap          $1.42T\n"
    "`For more details..."
)


def test_extract_whale_alert_long():
    results = extract_whale_alert(WHALE_LONG_TEXT)
    assert len(results) == 1
    r = results[0]
    assert r["type"] == "whale_alert"
    assert r["symbol"] == "ETH"
    assert r["direction"] == "Long"
    assert r["entry_price"] == "2188.07"


def test_extract_whale_alert_short():
    results = extract_whale_alert(WHALE_SHORT_TEXT)
    assert len(results) == 1
    r = results[0]
    assert r["type"] == "whale_alert"
    assert r["symbol"] == "ETH"
    assert r["direction"] == "Short"
    assert r["entry_price"] == "2186.95"


def test_extract_symbol_block():
    results = extract_symbol_block(SYMBOL_BLOCK_TEXT)
    assert len(results) == 1
    r = results[0]
    assert r["type"] == "symbol_info"
    assert r["symbol"] == "BTC"
    assert r["entry_price"] == "70859.90"
    assert r["direction"] is None


def test_extract_whale_alert_no_match():
    assert extract_whale_alert("Hello world") == []


def test_extract_symbol_block_no_match():
    assert extract_symbol_block("Random text") == []


def test_parse_row_whale():
    row = {
        "id": "1",
        "date": "2026-04-12",
        "text": WHALE_LONG_TEXT,
        "has_photo": "False",
        "photo_path": "",
    }
    results = parse_row(row)
    assert len(results) == 1
    assert results[0]["symbol"] == "ETH"
    assert results[0]["entry_price"] == "2188.07"
    assert results[0]["message_id"] == "1"


def test_parse_row_symbol_info():
    row = {
        "id": "2",
        "date": "2026-04-12",
        "text": SYMBOL_BLOCK_TEXT,
        "has_photo": "False",
        "photo_path": "",
    }
    results = parse_row(row)
    assert len(results) == 1
    assert results[0]["symbol"] == "BTC"
    assert results[0]["entry_price"] == "70859.90"


def test_deduplication(tmp_path):
    csv_content = (
        "id,date,sender_id,text,has_photo,photo_path,chat_id\n"
        f'1,2026-04-12,123,"{WHALE_LONG_TEXT}",False,,Chat\n'
        f'1,2026-04-12,123,"{WHALE_LONG_TEXT}",False,,Chat\n'
        f'2,2026-04-12,123,"{SYMBOL_BLOCK_TEXT}",False,,Chat\n'
    )
    input_file = tmp_path / "test_messages.csv"
    input_file.write_text(csv_content)
    output_file = tmp_path / "test_extracted.csv"

    results = main(str(input_file), str(output_file))
    assert len(results) == 2, f"Expected 2, got {len(results)}: {results}"


def test_main_on_real_data():
    real_csv = os.path.join(os.path.dirname(__file__), "messages.csv")
    if not os.path.exists(real_csv):
        pytest.skip("messages.csv not found")

    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        output_path = f.name
    try:
        results = main(real_csv, output_path)
        assert len(results) > 0
        symbols = {r["symbol"] for r in results}
        assert "ETH" in symbols or "BTC" in symbols or "SOL" in symbols
    finally:
        os.unlink(output_path)
