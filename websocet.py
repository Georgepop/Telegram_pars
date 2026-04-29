"""Collect closed Binance USD-M futures klines from the websocket API."""

import asyncio
import json
import os
import time
from datetime import datetime
from typing import Any, Callable, Iterable, Sequence
from urllib.request import urlopen


EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
WEBSOCKET_BASE_URL = "wss://fstream.binance.com"
SOCKET_ROUTE = "market"
STREAM_INTERVAL = os.getenv("BINANCE_KLINE_INTERVAL", "1m")
BINANCE_MAX_STREAMS_PER_CONNECTION = 1024


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        return default
    return max(value, minimum)


BATCH_SIZE = _env_int("BATCH_SIZE", 1000)
FLUSH_INTERVAL_SECONDS = _env_int("FLUSH_INTERVAL_SECONDS", 60)
RECONNECT_DELAY_SECONDS = _env_int("RECONNECT_DELAY_SECONDS", 3)
MAX_RECONNECT_DELAY_SECONDS = _env_int("MAX_RECONNECT_DELAY_SECONDS", 300)
CONNECTION_REFRESH_SECONDS = _env_int("CONNECTION_REFRESH_SECONDS", 23 * 60 * 60)
MAX_STREAMS_PER_CONNECTION = min(
    _env_int("MAX_STREAMS_PER_CONNECTION", BINANCE_MAX_STREAMS_PER_CONNECTION),
    BINANCE_MAX_STREAMS_PER_CONNECTION,
)

insert_buffer: list[dict[str, Any]] = []


def parse_exchange_symbols(exchange_info: dict[str, Any]) -> list[str]:
    """Return active USD-M USDT symbols in Binance websocket format."""
    symbols = set()
    for item in exchange_info.get("symbols", []):
        symbol = str(item.get("symbol", "")).strip()
        if item.get("status") == "TRADING" and symbol.endswith("USDT"):
            symbols.add(symbol.lower())
    return sorted(symbols)


def get_usdt_symbols(
    exchange_info_url: str = EXCHANGE_INFO_URL,
    timeout: int = 20,
) -> list[str]:
    """Fetch tradable USDT futures symbols from Binance."""
    with urlopen(exchange_info_url, timeout=timeout) as response:
        exchange_info = json.loads(response.read().decode("utf-8"))
    return parse_exchange_symbols(exchange_info)


def build_stream_names(symbols: Sequence[str], interval: str = STREAM_INTERVAL) -> list[str]:
    return [
        f"{symbol.strip().lower()}@kline_{interval}"
        for symbol in symbols
        if symbol and symbol.strip()
    ]


def _chunks(items: Sequence[str], size: int) -> Iterable[list[str]]:
    if size < 1:
        raise ValueError("Chunk size must be positive")
    for start in range(0, len(items), size):
        yield list(items[start:start + size])


def build_socket_url(stream_names: Sequence[str]) -> str:
    if not stream_names:
        raise ValueError("No streams were provided")
    return f"{WEBSOCKET_BASE_URL}/{SOCKET_ROUTE}/stream?streams={'/'.join(stream_names)}"


def build_socket_urls(
    symbols: Sequence[str],
    interval: str = STREAM_INTERVAL,
    streams_per_connection: int = MAX_STREAMS_PER_CONNECTION,
) -> list[str]:
    stream_names = build_stream_names(symbols, interval=interval)
    if not stream_names:
        raise ValueError("No symbols available for websocket subscription")

    streams_per_connection = min(streams_per_connection, BINANCE_MAX_STREAMS_PER_CONNECTION)
    return [
        build_socket_url(chunk)
        for chunk in _chunks(stream_names, streams_per_connection)
    ]


def parse_kline(data: dict[str, Any]) -> dict[str, Any]:
    """Parse a Binance kline event into the existing storage shape."""
    kline = data["k"]
    close_timestamp = kline["T"] / 1000
    return {
        "s": data.get("s") or kline.get("s"),
        "d": close_timestamp,
        "h": float(kline["h"]),
        "l": float(kline["l"]),
        "t": datetime.fromtimestamp(close_timestamp),
        "o": float(kline["o"]),
        "c": float(kline["c"]),
        "v": float(kline["v"]),
        "is_closed": bool(kline["x"]),
    }


def flush_buffer(
    collection: Any | None = None,
    buffer: list[dict[str, Any]] | None = None,
) -> int:
    """Insert buffered klines into MongoDB and clear the buffer after success."""
    target_buffer = insert_buffer if buffer is None else buffer
    if not target_buffer or collection is None:
        return 0

    docs = list(target_buffer)
    collection.insert_many(docs, ordered=False)
    target_buffer.clear()
    return len(docs)


def process_kline_event(
    event: dict[str, Any],
    collection: Any | None = None,
    buffer: list[dict[str, Any]] | None = None,
    batch_size: int = BATCH_SIZE,
    printer: Callable[[Any], None] | None = print,
) -> dict[str, Any] | None:
    if event.get("e") != "kline":
        return None

    kline = parse_kline(event)
    if not kline["is_closed"]:
        return None

    if printer is not None:
        printer(kline)

    if collection is not None:
        target_buffer = insert_buffer if buffer is None else buffer
        target_buffer.append(kline)
        if len(target_buffer) >= batch_size:
            flush_buffer(collection, target_buffer)

    return kline


def process_market_message(
    message: str,
    collection: Any | None = None,
    buffer: list[dict[str, Any]] | None = None,
    batch_size: int = BATCH_SIZE,
    printer: Callable[[Any], None] | None = print,
) -> dict[str, Any] | None:
    payload = json.loads(message)
    event = payload.get("data", payload)
    if not isinstance(event, dict):
        return None
    return process_kline_event(
        event,
        collection=collection,
        buffer=buffer,
        batch_size=batch_size,
        printer=printer,
    )


def get_mongo_collection_from_env() -> Any | None:
    uri = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
    if not uri:
        return None

    try:
        from pymongo import MongoClient
    except ImportError as exc:
        raise RuntimeError(
            "pymongo is required when MONGODB_URI or MONGO_URI is configured"
        ) from exc

    db_name = os.getenv("MONGODB_DB") or os.getenv("MONGODB_DATABASE") or "binance"
    collection_name = os.getenv("MONGODB_COLLECTION", "symbols")
    return MongoClient(uri)[db_name][collection_name]


def _load_websockets_module() -> Any:
    try:
        import websockets
    except ImportError as exc:
        raise RuntimeError(
            "websockets is required to run the Binance stream collector"
        ) from exc
    return websockets


def _is_invalid_uri_error(websockets_module: Any, exc: Exception) -> bool:
    invalid_uri = getattr(websockets_module.exceptions, "InvalidURI", None)
    return invalid_uri is not None and isinstance(exc, invalid_uri)


def calculate_reconnect_delay(
    attempt: int,
    base_delay: int = RECONNECT_DELAY_SECONDS,
    max_delay: int = MAX_RECONNECT_DELAY_SECONDS,
) -> int:
    return min(max_delay, base_delay * (2 ** max(attempt, 0)))


async def consume_socket(
    socket_url: str,
    collection: Any | None = None,
    batch_size: int = BATCH_SIZE,
    flush_interval_seconds: int = FLUSH_INTERVAL_SECONDS,
    reconnect_delay_seconds: int = RECONNECT_DELAY_SECONDS,
    max_reconnect_delay_seconds: int = MAX_RECONNECT_DELAY_SECONDS,
    connection_refresh_seconds: int = CONNECTION_REFRESH_SECONDS,
    printer: Callable[[Any], None] | None = print,
) -> None:
    websockets = _load_websockets_module()
    buffer: list[dict[str, Any]] = []
    reconnect_attempt = 0

    while True:
        try:
            if printer is not None:
                printer(f"Connecting to {socket_url}")

            async with websockets.connect(
                socket_url,
                ping_interval=None,
                close_timeout=10,
            ) as ws:
                reconnect_attempt = 0
                connected_at = time.monotonic()
                last_flush_at = connected_at

                while True:
                    now = time.monotonic()
                    if now - connected_at >= connection_refresh_seconds:
                        if printer is not None:
                            printer("Refreshing websocket connection before 24h limit")
                        break

                    message = await ws.recv()
                    process_market_message(
                        message,
                        collection=collection,
                        buffer=buffer,
                        batch_size=batch_size,
                        printer=printer,
                    )

                    now = time.monotonic()
                    if collection is not None and now - last_flush_at >= flush_interval_seconds:
                        inserted = flush_buffer(collection, buffer)
                        if inserted and printer is not None:
                            printer(f"Flushed {inserted} klines")
                        last_flush_at = now

        except asyncio.CancelledError:
            flush_buffer(collection, buffer)
            raise
        except Exception as exc:
            if _is_invalid_uri_error(websockets, exc):
                if printer is not None:
                    printer(f"Invalid websocket URI: {exc}")
                break
            if printer is not None:
                printer(f"Stream error: {type(exc).__name__}: {exc}")
        finally:
            try:
                flush_buffer(collection, buffer)
            except Exception as exc:
                if printer is not None:
                    printer(f"DB flush error: {type(exc).__name__}: {exc}")

        delay = calculate_reconnect_delay(
            reconnect_attempt,
            base_delay=reconnect_delay_seconds,
            max_delay=max_reconnect_delay_seconds,
        )
        reconnect_attempt += 1
        if printer is not None:
            printer(f"Reconnecting in {delay}s...")
        await asyncio.sleep(delay)


async def futures_kline_stream(
    symbols: Sequence[str] | None = None,
    collection: Any | None = None,
    interval: str = STREAM_INTERVAL,
    streams_per_connection: int = MAX_STREAMS_PER_CONNECTION,
    printer: Callable[[Any], None] | None = print,
) -> None:
    if symbols is None:
        symbols = get_usdt_symbols()
    if collection is None:
        collection = get_mongo_collection_from_env()

    socket_urls = build_socket_urls(
        symbols,
        interval=interval,
        streams_per_connection=streams_per_connection,
    )

    if printer is not None:
        printer(
            f"Subscribing to {len(symbols)} symbols across "
            f"{len(socket_urls)} websocket connection(s)"
        )
        if collection is None:
            printer("MONGODB_URI is not set; closed klines will be printed only")

    await asyncio.gather(*[
        consume_socket(socket_url, collection=collection, printer=printer)
        for socket_url in socket_urls
    ])


if __name__ == "__main__":
    try:
        asyncio.run(futures_kline_stream())
    except KeyboardInterrupt:
        print("Stopped")
