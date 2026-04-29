"""Tests for the Binance futures websocket collector."""

import json
from datetime import datetime

import pytest


def _closed_kline_event(is_closed=True):
    return {
        "e": "kline",
        "E": 1638747660000,
        "s": "BTCUSDT",
        "k": {
            "t": 1638747660000,
            "T": 1638747719999,
            "s": "BTCUSDT",
            "i": "1m",
            "o": "0.0010",
            "c": "0.0020",
            "h": "0.0025",
            "l": "0.0015",
            "v": "1000",
            "x": is_closed,
        },
    }


def test_websocet_import_is_side_effect_free():
    import websocet

    assert websocet.SOCKET_ROUTE == "market"


def test_parse_exchange_symbols_filters_trading_usdt_pairs():
    import websocet

    payload = {
        "symbols": [
            {"symbol": "BTCUSDT", "status": "TRADING"},
            {"symbol": "ETHUSDT", "status": "BREAK"},
            {"symbol": "ETHBTC", "status": "TRADING"},
            {"symbol": "SOLUSDT", "status": "TRADING"},
        ]
    }

    assert websocet.parse_exchange_symbols(payload) == ["btcusdt", "solusdt"]


def test_build_socket_urls_lowercases_and_chunks_streams():
    import websocet

    urls = websocet.build_socket_urls(
        ["BTCUSDT", "ETHUSDT"],
        interval="1m",
        streams_per_connection=1,
    )

    assert urls == [
        "wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1m",
        "wss://fstream.binance.com/market/stream?streams=ethusdt@kline_1m",
    ]


def test_parse_kline_keeps_existing_output_shape():
    import websocet

    result = websocet.parse_kline(_closed_kline_event())

    assert result == {
        "s": "BTCUSDT",
        "d": 1638747719.999,
        "h": 0.0025,
        "l": 0.0015,
        "t": datetime.fromtimestamp(1638747719.999),
        "o": 0.001,
        "c": 0.002,
        "v": 1000.0,
        "is_closed": True,
    }


class FakeCollection:
    def __init__(self):
        self.inserted = []
        self.ordered = None

    def insert_many(self, docs, ordered=False):
        self.inserted.extend(docs)
        self.ordered = ordered


def test_process_market_message_flushes_closed_kline_batches():
    import websocet

    collection = FakeCollection()
    buffer = []
    message = {"data": _closed_kline_event()}

    kline = websocet.process_market_message(
        json.dumps(message),
        collection=collection,
        buffer=buffer,
        batch_size=1,
        printer=None,
    )

    assert kline["s"] == "BTCUSDT"
    assert collection.inserted == [kline]
    assert collection.ordered is False
    assert buffer == []


def test_process_market_message_ignores_open_klines():
    import websocet

    collection = FakeCollection()
    buffer = []
    message = {"data": _closed_kline_event(is_closed=False)}

    assert websocet.process_market_message(
        json.dumps(message),
        collection=collection,
        buffer=buffer,
        batch_size=1,
        printer=None,
    ) is None
    assert collection.inserted == []
    assert buffer == []


def test_build_socket_urls_rejects_empty_symbols():
    import websocet

    with pytest.raises(ValueError, match="No symbols"):
        websocet.build_socket_urls([])


def test_calculate_reconnect_delay_uses_capped_backoff():
    import websocet

    assert websocet.calculate_reconnect_delay(0, base_delay=3, max_delay=60) == 3
    assert websocet.calculate_reconnect_delay(1, base_delay=3, max_delay=60) == 6
    assert websocet.calculate_reconnect_delay(2, base_delay=3, max_delay=60) == 12
    assert websocet.calculate_reconnect_delay(10, base_delay=3, max_delay=60) == 60
