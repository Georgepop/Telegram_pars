"""
Parse messages.csv and extract symbol, entry price, leverage, and side from:
- Whale Alert text messages (e.g., "** Long ** **ETH** with **6x** leverage, entry price **$2188.07**")
- Symbol info messages (e.g., "`Symbol              BTC\nPrice               $70859.90")
- Trading position photos (e.g., screenshots showing "SOL LONG 20X", "Entry Price: 85,956")
"""

import base64
import csv
import json
import os
import re
import sys

import requests

_usdt_symbols = None


def _load_usdt_symbols():
    """Fetch and cache all active USDT perpetual symbols from Binance."""
    global _usdt_symbols
    if _usdt_symbols is None:
        try:
            data = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo', timeout=10).json()
            raw = [x["symbol"] for x in data["symbols"] if x['status'] == 'TRADING']
            _usdt_symbols = sorted([i.upper() for i in raw if i.upper().endswith('USDT')])
        except Exception:
            _usdt_symbols = []
    return _usdt_symbols


def check_usdt_pair(text):
    """Return the most specific USDT pair symbol mentioned in *text*, or None.

    Fetches the active Binance USDT perpetual symbol list on first call and
    caches it for subsequent calls.  When multiple symbols match, the one with
    the longest base token (e.g. TRUMPUSDT over BTCUSDT when both bases appear)
    is returned so that compound ticker names are preferred over short ones.
    """
    symbols = _load_usdt_symbols()
    matched = [s for s in symbols if s[:-4] in str(text).upper()]
    if not matched:
        return None
    return max(matched, key=lambda s: len(s))


WHALE_ALERT_PATTERN = re.compile(
    r"\*\*Whale Alert:\*\*.*?"
    r"\*\*\s*(?P<direction>Long|Short)\s*\*\*\s+\*\*(?P<symbol>[A-Z0-9]+)\*\*"
    r".*?with\s+\*\*(?P<leverage>\d+)x\*\*\s+leverage.*?"
    r"entry price\s+\*\*\$(?P<entry_price>[\d,]+\.?\d*)\*\*",
    re.IGNORECASE | re.DOTALL,
)

SYMBOL_BLOCK_PATTERN = re.compile(
    r"`Symbol\s+(?P<symbol>[A-Z0-9]+)\s*\n"
    r"Price\s+\$(?P<price>[\d,]+\.?\d*)",
    re.IGNORECASE,
)

_anthropic_client = None


def _get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        import anthropic
        _anthropic_client = anthropic.Anthropic()
    return _anthropic_client


def extract_whale_alert(text):
    """Return list of dicts from Whale Alert messages."""
    results = []
    for m in WHALE_ALERT_PATTERN.finditer(text):
        results.append({
            "type": "whale_alert",
            "symbol": m.group("symbol").upper(),
            "direction": m.group("direction").capitalize(),
            "leverage": m.group("leverage"),
            "entry_price": m.group("entry_price").replace(",", ""),
        })
    return results


def extract_symbol_block(text):
    """Return list of dicts from symbol info blocks."""
    results = []
    for m in SYMBOL_BLOCK_PATTERN.finditer(text):
        results.append({
            "type": "symbol_info",
            "symbol": m.group("symbol").upper(),
            "entry_price": m.group("price").replace(",", ""),
            "direction": None,
            "leverage": None,
        })
    return results


def extract_from_photo(photo_path, base_dir="."):
    """Extract trading fields from a photo using Claude vision.

    Returns a dict with keys type, symbol, direction, leverage, entry_price,
    or None if the photo contains no recognizable trading position data.
    """
    full_path = os.path.join(base_dir, photo_path) if not os.path.isabs(photo_path) else photo_path
    if not os.path.exists(full_path):
        return None

    ext = os.path.splitext(full_path)[1].lower()
    media_type_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".webp": "image/webp"}
    media_type = media_type_map.get(ext, "image/jpeg")

    with open(full_path, "rb") as f:
        image_data = base64.standard_b64encode(f.read()).decode("utf-8")

    client = _get_anthropic_client()
    response = client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=256,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": image_data,
                    },
                },
                {
                    "type": "text",
                    "text": (
                        "This image may be a trading position screenshot. "
                        "If it shows a trading position with symbol, side (Long/Short), leverage, and/or entry price, "
                        "extract those fields and respond with JSON only:\n"
                        '{"symbol": "...", "direction": "Long" or "Short", "leverage": "20" (number only, no X), "entry_price": "85956.00"}\n'
                        "Use null for any field not visible. Numbers only (no commas, $, or X).\n"
                        "If the image is NOT a trading position screenshot (e.g. a chart, meme, or unrelated photo), "
                        'respond with: {"not_trading": true}'
                    ),
                },
            ],
        }],
    )

    text = next((b.text for b in response.content if b.type == "text"), "").strip()
    # Extract JSON from the response
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None

    try:
        data = json.loads(match.group())
    except json.JSONDecodeError:
        return None

    if data.get("not_trading"):
        return None

    symbol = data.get("symbol")
    if not symbol:
        return None

    # Normalize entry_price: strip commas and currency symbols
    raw_price = str(data.get("entry_price") or "")
    entry_price = re.sub(r"[,$]", "", raw_price) or None

    # Normalize leverage: digits only
    raw_leverage = str(data.get("leverage") or "")
    leverage = re.sub(r"[^0-9]", "", raw_leverage) or None

    direction = data.get("direction")
    if direction:
        direction = direction.capitalize()

    return {
        "type": "photo",
        "symbol": symbol.upper(),
        "direction": direction,
        "leverage": leverage,
        "entry_price": entry_price,
    }


def parse_row(row, base_dir=".", use_vision=True):
    text = row.get("text", "") or ""
    results = []
    results.extend(extract_whale_alert(text))
    results.extend(extract_symbol_block(text))

    has_photo = row.get("has_photo", "").strip().lower() == "true"
    photo_path = row.get("photo_path", "").strip()
    if use_vision and has_photo and photo_path:
        photo_result = extract_from_photo(photo_path, base_dir=base_dir)
        if photo_result:
            results.append(photo_result)

    for r in results:
        r["message_id"] = row.get("id", "")
        r["date"] = row.get("date", "")
        r["has_photo"] = row.get("has_photo", "")
        r["photo_path"] = row.get("photo_path", "")
    return results


def main(input_path="messages.csv", output_path="extracted.csv", use_vision=True):
    base_dir = os.path.dirname(os.path.abspath(input_path))
    seen_ids = set()
    all_results = []
    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            msg_id = row.get("id", "")
            if msg_id in seen_ids:
                continue
            seen_ids.add(msg_id)
            all_results.extend(parse_row(row, base_dir=base_dir, use_vision=use_vision))

    fieldnames = ["message_id", "date", "type", "symbol", "direction", "leverage", "entry_price", "has_photo", "photo_path"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)

    print(f"Extracted {len(all_results)} records to {output_path}")
    return all_results


if __name__ == "__main__":
    input_path = sys.argv[1] if len(sys.argv) > 1 else "messages.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "extracted.csv"
    main(input_path, output_path)
