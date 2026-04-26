"""Tests for parse_messages.py"""

import csv
import io
import sys
import os
from unittest.mock import MagicMock, patch
import pytest

sys.path.insert(0, os.path.dirname(__file__))
from parse_messages import extract_whale_alert, extract_symbol_block, extract_from_photo, parse_row, main


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


def _make_anthropic_response(text):
    """Build a minimal mock Anthropic response object."""
    content_block = MagicMock()
    content_block.text = text
    response = MagicMock()
    response.content = [content_block]
    return response


def test_extract_from_photo_no_key(tmp_path):
    """Returns [] when ANTHROPIC_API_KEY is not set."""
    img = tmp_path / "signal.jpg"
    img.write_bytes(b"\xff\xd8\xff")  # minimal JPEG header
    import parse_messages
    parse_messages._anthropic_client = None
    with patch.dict(os.environ, {}, clear=True):
        os.environ.pop("ANTHROPIC_API_KEY", None)
        result = extract_from_photo(str(img))
    assert result == []


def test_extract_from_photo_missing_file():
    """Returns [] when photo path does not exist."""
    with patch("parse_messages._get_anthropic_client") as mock_client:
        mock_client.return_value = MagicMock()
        result = extract_from_photo("/nonexistent/path.jpg")
    assert result == []


def test_extract_from_photo_no_signal(tmp_path):
    """Returns [] when Claude says NO_SIGNAL."""
    img = tmp_path / "chart.jpg"
    img.write_bytes(b"\xff\xd8\xff")

    mock_client = MagicMock()
    mock_client.messages.create.return_value = _make_anthropic_response("NO_SIGNAL")

    with patch("parse_messages._get_anthropic_client", return_value=mock_client):
        result = extract_from_photo(str(img))

    assert result == []


def test_extract_from_photo_long_signal(tmp_path):
    """Parses a Long trading signal from Claude's response."""
    img = tmp_path / "sol_long.jpg"
    img.write_bytes(b"\xff\xd8\xff")

    mock_client = MagicMock()
    mock_client.messages.create.return_value = _make_anthropic_response(
        "SYMBOL=SOL DIRECTION=Long ENTRY_PRICE=85956"
    )

    with patch("parse_messages._get_anthropic_client", return_value=mock_client):
        result = extract_from_photo(str(img))

    assert len(result) == 1
    r = result[0]
    assert r["type"] == "photo_signal"
    assert r["symbol"] == "SOL"
    assert r["direction"] == "Long"
    assert r["entry_price"] == "85956"


def test_extract_from_photo_short_signal(tmp_path):
    """Parses a Short trading signal from Claude's response."""
    img = tmp_path / "btc_short.jpg"
    img.write_bytes(b"\xff\xd8\xff")

    mock_client = MagicMock()
    mock_client.messages.create.return_value = _make_anthropic_response(
        "SYMBOL=BTC DIRECTION=Short ENTRY_PRICE=77500.50"
    )

    with patch("parse_messages._get_anthropic_client", return_value=mock_client):
        result = extract_from_photo(str(img))

    assert len(result) == 1
    assert result[0]["symbol"] == "BTC"
    assert result[0]["direction"] == "Short"
    assert result[0]["entry_price"] == "77500.50"


def test_parse_row_falls_back_to_photo(tmp_path):
    """parse_row calls extract_from_photo when has_photo=True and no text match."""
    img = tmp_path / "signal.jpg"
    img.write_bytes(b"\xff\xd8\xff")

    row = {
        "id": "42",
        "date": "2026-04-26",
        "text": "",
        "has_photo": "True",
        "photo_path": str(img),
    }

    mock_client = MagicMock()
    mock_client.messages.create.return_value = _make_anthropic_response(
        "SYMBOL=ETH DIRECTION=Long ENTRY_PRICE=2500.00"
    )

    with patch("parse_messages._get_anthropic_client", return_value=mock_client):
        results = parse_row(row)

    assert len(results) == 1
    assert results[0]["symbol"] == "ETH"
    assert results[0]["message_id"] == "42"
    assert results[0]["type"] == "photo_signal"


def test_parse_row_skips_photo_when_text_matched():
    """parse_row does NOT call extract_from_photo when text already matched."""
    row = {
        "id": "99",
        "date": "2026-04-26",
        "text": WHALE_LONG_TEXT,
        "has_photo": "True",
        "photo_path": "/some/image.jpg",
    }

    with patch("parse_messages.extract_from_photo") as mock_photo:
        results = parse_row(row)

    mock_photo.assert_not_called()
    assert len(results) == 1
    assert results[0]["symbol"] == "ETH"


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
