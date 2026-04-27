"""Tests for parse_messages.py"""

import csv
import io
import sys
import os
import runpy
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(__file__))
from parse_messages import extract_whale_alert, extract_symbol_block, extract_from_photo, parse_row, main, check_usdt_pair


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

DOWNLOADS_DIR = os.path.join(os.path.dirname(__file__), "downloads")
PHOTO_SOL = os.path.join(DOWNLOADS_DIR, "INVEST ZONE Chat 💬_20260426_045028.jpg")
PHOTO_TRUMP = os.path.join(DOWNLOADS_DIR, "INVEST ZONE Chat 💬_20260426_053249.jpg")
PHOTO_BTC = os.path.join(DOWNLOADS_DIR, "INVEST ZONE Chat 💬_20260426_053335.jpg")
PHOTO_ETH = os.path.join(DOWNLOADS_DIR, "INVEST ZONE Chat 💬_20260426_053403.jpg")
PHOTO_CHART = os.path.join(DOWNLOADS_DIR, "INVEST ZONE Chat 💬_20260426_053504.jpg")
PHOTO_UNRELATED = os.path.join(DOWNLOADS_DIR, "INVEST ZONE Chat 💬_20260426_053358.jpg")


def test_extract_whale_alert_long():
    results = extract_whale_alert(WHALE_LONG_TEXT)
    assert len(results) == 1
    r = results[0]
    assert r["type"] == "whale_alert"
    assert r["symbol"] == "ETH"
    assert r["direction"] == "Long"
    assert r["leverage"] == "6"
    assert r["entry_price"] == "2188.07"


def test_extract_whale_alert_short():
    results = extract_whale_alert(WHALE_SHORT_TEXT)
    assert len(results) == 1
    r = results[0]
    assert r["type"] == "whale_alert"
    assert r["symbol"] == "ETH"
    assert r["direction"] == "Short"
    assert r["leverage"] == "15"
    assert r["entry_price"] == "2186.95"


def test_extract_symbol_block():
    results = extract_symbol_block(SYMBOL_BLOCK_TEXT)
    assert len(results) == 1
    r = results[0]
    assert r["type"] == "symbol_info"
    assert r["symbol"] == "BTC"
    assert r["entry_price"] == "70859.90"
    assert r["direction"] is None
    assert r["leverage"] is None


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
    results = parse_row(row, use_vision=False)
    assert len(results) == 1
    assert results[0]["symbol"] == "ETH"
    assert results[0]["leverage"] == "6"
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
    results = parse_row(row, use_vision=False)
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

    results = main(str(input_file), str(output_file), use_vision=False)
    assert len(results) == 2, f"Expected 2, got {len(results)}: {results}"


def test_main_on_real_data():
    real_csv = os.path.join(os.path.dirname(__file__), "messages.csv")
    if not os.path.exists(real_csv):
        pytest.skip("messages.csv not found")

    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        output_path = f.name
    try:
        results = main(real_csv, output_path, use_vision=False)
        assert len(results) > 0
        symbols = {r["symbol"] for r in results}
        assert "ETH" in symbols or "BTC" in symbols or "SOL" in symbols
    finally:
        os.unlink(output_path)


# --- Photo extraction tests ---

def _make_photo_api_response(symbol, direction, leverage, entry_price):
    """Build a mock Anthropic API response for photo extraction."""
    import json
    payload = {"symbol": symbol, "direction": direction, "leverage": leverage, "entry_price": entry_price}
    mock_block = MagicMock()
    mock_block.type = "text"
    mock_block.text = json.dumps(payload)
    mock_response = MagicMock()
    mock_response.content = [mock_block]
    return mock_response


def _make_not_trading_response():
    mock_block = MagicMock()
    mock_block.type = "text"
    mock_block.text = '{"not_trading": true}'
    mock_response = MagicMock()
    mock_response.content = [mock_block]
    return mock_response


@pytest.mark.skipif(not os.path.exists(PHOTO_SOL), reason="SOL photo not available")
def test_extract_from_photo_sol_with_mock():
    mock_response = _make_photo_api_response("SOL", "Long", "20", "85956")
    with patch("parse_messages._get_anthropic_client") as mock_client_fn:
        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_response
        mock_client_fn.return_value = mock_client
        result = extract_from_photo(PHOTO_SOL)

    assert result is not None
    assert result["symbol"] == "SOL"
    assert result["direction"] == "Long"
    assert result["leverage"] == "20"
    assert result["entry_price"] == "85956"
    assert result["type"] == "photo"


@pytest.mark.skipif(not os.path.exists(PHOTO_TRUMP), reason="TRUMP photo not available")
def test_extract_from_photo_trump_with_mock():
    mock_response = _make_photo_api_response("TRUMPUSDT", "Long", "75", None)
    with patch("parse_messages._get_anthropic_client") as mock_client_fn:
        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_response
        mock_client_fn.return_value = mock_client
        result = extract_from_photo(PHOTO_TRUMP)

    assert result is not None
    assert result["symbol"] == "TRUMPUSDT"
    assert result["direction"] == "Long"
    assert result["leverage"] == "75"


@pytest.mark.skipif(not os.path.exists(PHOTO_CHART), reason="Chart photo not available")
def test_extract_from_photo_not_trading_returns_none():
    with patch("parse_messages._get_anthropic_client") as mock_client_fn:
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _make_not_trading_response()
        mock_client_fn.return_value = mock_client
        result = extract_from_photo(PHOTO_CHART)

    assert result is None


def test_extract_from_photo_missing_file():
    result = extract_from_photo("/nonexistent/path/photo.jpg")
    assert result is None


def test_parse_row_with_photo_vision(tmp_path):
    """parse_row calls extract_from_photo when has_photo=True."""
    fake_photo = tmp_path / "test.jpg"
    fake_photo.write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100)

    mock_response = _make_photo_api_response("BTC", "Long", "100", "97800")
    with patch("parse_messages._get_anthropic_client") as mock_client_fn:
        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_response
        mock_client_fn.return_value = mock_client

        row = {
            "id": "99",
            "date": "2026-04-26",
            "text": "",
            "has_photo": "True",
            "photo_path": str(fake_photo),
        }
        results = parse_row(row, base_dir="", use_vision=True)

    photo_results = [r for r in results if r["type"] == "photo"]
    assert len(photo_results) == 1
    r = photo_results[0]
    assert r["symbol"] == "BTC"
    assert r["direction"] == "Long"
    assert r["leverage"] == "100"
    assert r["entry_price"] == "97800"
    assert r["message_id"] == "99"


@pytest.mark.skipif(
    not os.path.exists(PHOTO_SOL),
    reason="Sample photos not available for live API test",
)
@pytest.mark.skipif(
    not os.environ.get("ANTHROPIC_API_KEY"),
    reason="ANTHROPIC_API_KEY not set — skipping live API test",
)
def test_extract_from_photo_sol_live():
    """Live API test: extract trading fields from the SOL position photo."""
    result = extract_from_photo(PHOTO_SOL)
    assert result is not None, "Expected trading data from SOL position photo"
    assert result["symbol"] in ("SOL", "SOLUSDT", "SOL/USDT")
    assert result["direction"] in ("Long", "Short")
    assert result["leverage"] is not None
    assert result["entry_price"] is not None


@pytest.mark.skipif(
    not os.path.exists(PHOTO_BTC),
    reason="Sample photos not available for live API test",
)
@pytest.mark.skipif(
    not os.environ.get("ANTHROPIC_API_KEY"),
    reason="ANTHROPIC_API_KEY not set — skipping live API test",
)
def test_extract_from_photo_btc_live():
    """Live API test: extract trading fields from the BTCUSDT position photo."""
    result = extract_from_photo(PHOTO_BTC)
    assert result is not None
    assert "BTC" in result["symbol"]
    assert result["direction"] == "Long"
    assert result["leverage"] == "100"


# --- check_usdt_pair tests ---

_FAKE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "TRUMPUSDT", "XRPUSDT"]


def _fake_exchange_info(symbols):
    class FakeResponse:
        def json(self):
            return {
                "symbols": [
                    {"symbol": symbol, "status": "TRADING"}
                    for symbol in symbols
                ]
            }

    return FakeResponse()


def test_check_usdt_pair_btc(monkeypatch):
    monkeypatch.setattr("parse_messages._usdt_symbols", _FAKE_SYMBOLS)
    assert check_usdt_pair("BTC is pumping today") == "BTCUSDT"


def test_check_usdt_pair_eth(monkeypatch):
    monkeypatch.setattr("parse_messages._usdt_symbols", _FAKE_SYMBOLS)
    assert check_usdt_pair("ETH long 6x") == "ETHUSDT"


def test_check_usdt_pair_sol(monkeypatch):
    monkeypatch.setattr("parse_messages._usdt_symbols", _FAKE_SYMBOLS)
    assert check_usdt_pair("SOL LONG 20X entry 85956") == "SOLUSDT"


def test_check_usdt_pair_longest_wins(monkeypatch):
    """When text contains 'TRUMP', TRUMPUSDT should win over any shorter match."""
    monkeypatch.setattr("parse_messages._usdt_symbols", _FAKE_SYMBOLS)
    assert check_usdt_pair("TRUMP position") == "TRUMPUSDT"


def test_check_usdt_pair_no_match(monkeypatch):
    monkeypatch.setattr("parse_messages._usdt_symbols", _FAKE_SYMBOLS)
    assert check_usdt_pair("random unrelated text") is None


def test_check_usdt_pair_case_insensitive(monkeypatch):
    monkeypatch.setattr("parse_messages._usdt_symbols", _FAKE_SYMBOLS)
    assert check_usdt_pair("btc long") == "BTCUSDT"


def test_check_usdt_pair_empty_string(monkeypatch):
    monkeypatch.setattr("parse_messages._usdt_symbols", _FAKE_SYMBOLS)
    assert check_usdt_pair("") is None


def test_usdssss_check_usdt_pair_loads_lazily_and_caches(monkeypatch):
    calls = []

    def fake_get(url, timeout):
        calls.append((url, timeout))
        return _fake_exchange_info(_FAKE_SYMBOLS)

    monkeypatch.setattr("requests.get", fake_get)
    module = runpy.run_path(os.path.join(os.path.dirname(__file__), "usdssss"))

    assert calls == []
    assert module["check_usdt_pair"]("btc long") == "BTCUSDT"
    assert module["check_usdt_pair"]("eth short") == "ETHUSDT"
    assert len(calls) == 1
    assert calls[0][1] == 10


def test_usdssss_check_keeps_backwards_compatible_name(monkeypatch):
    monkeypatch.setattr("requests.get", lambda url, timeout: _fake_exchange_info(_FAKE_SYMBOLS))
    module = runpy.run_path(os.path.join(os.path.dirname(__file__), "usdssss"))

    assert module["check"]("TRUMP position") == "TRUMPUSDT"
    assert module["check"]("nothing related") is None
