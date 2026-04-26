"""
Parse messages.csv and extract symbol and entry price from:
- Whale Alert text messages (e.g., "** Long ** **ETH** with **6x** leverage, entry price **$2188.07**")
- Symbol info messages (e.g., "`Symbol              BTC\nPrice               $70859.90")
- Photo captions containing the above patterns
- Photos analyzed via the Anthropic API (Claude vision) when ANTHROPIC_API_KEY is available
"""

import base64
import csv
import os
import re
import sys


WHALE_ALERT_PATTERN = re.compile(
    r"\*\*Whale Alert:\*\*.*?"
    r"\*\*\s*(?P<direction>Long|Short)\s*\*\*\s+\*\*(?P<symbol>[A-Z0-9]+)\*\*"
    r".*?entry price\s+\*\*\$(?P<entry_price>[\d,]+\.?\d*)\*\*",
    re.IGNORECASE | re.DOTALL,
)

SYMBOL_BLOCK_PATTERN = re.compile(
    r"`Symbol\s+(?P<symbol>[A-Z0-9]+)\s*\n"
    r"Price\s+\$(?P<price>[\d,]+\.?\d*)",
    re.IGNORECASE,
)

_anthropic_client = None


def _get_anthropic_client():
    """Return a cached Anthropic client, initialised from ANTHROPIC_API_KEY env var."""
    global _anthropic_client
    if _anthropic_client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            return None
        import anthropic
        _anthropic_client = anthropic.Anthropic(api_key=api_key)
    return _anthropic_client


def extract_whale_alert(text):
    """Return list of (symbol, direction, entry_price) from Whale Alert messages."""
    results = []
    for m in WHALE_ALERT_PATTERN.finditer(text):
        results.append({
            "type": "whale_alert",
            "symbol": m.group("symbol").upper(),
            "direction": m.group("direction").capitalize(),
            "entry_price": m.group("entry_price").replace(",", ""),
        })
    return results


def extract_symbol_block(text):
    """Return list of (symbol, price) from symbol info blocks."""
    results = []
    for m in SYMBOL_BLOCK_PATTERN.finditer(text):
        results.append({
            "type": "symbol_info",
            "symbol": m.group("symbol").upper(),
            "entry_price": m.group("price").replace(",", ""),
            "direction": None,
        })
    return results


def extract_from_photo(photo_path):
    """Use Claude vision to extract trading signal data from a photo.

    Returns a list with one dict if a trading signal is found, otherwise [].
    Requires ANTHROPIC_API_KEY to be set; returns [] silently when it is not.
    """
    client = _get_anthropic_client()
    if not client:
        return []
    if not photo_path or not os.path.exists(photo_path):
        return []

    with open(photo_path, "rb") as f:
        image_data = base64.standard_b64encode(f.read()).decode("utf-8")

    ext = os.path.splitext(photo_path)[1].lower()
    media_type_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".webp": "image/webp"}
    media_type = media_type_map.get(ext, "image/jpeg")

    prompt = (
        "This image may contain a cryptocurrency trading signal. "
        "If it does, extract the following fields and reply with ONLY a single line in this exact format:\n"
        "SYMBOL=<symbol> DIRECTION=<Long|Short> ENTRY_PRICE=<number>\n"
        "Use the base symbol without USDT/USD suffix (e.g. BTC not BTCUSDT). "
        "If the image does not contain a clear trading signal with an entry price, reply with: NO_SIGNAL"
    )

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=64,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": image_data}},
                    {"type": "text", "text": prompt},
                ],
            }
        ],
    )

    reply = message.content[0].text.strip()
    if reply == "NO_SIGNAL" or not reply.startswith("SYMBOL="):
        return []

    parsed = {}
    for token in reply.split():
        if "=" in token:
            k, _, v = token.partition("=")
            parsed[k] = v

    symbol = parsed.get("SYMBOL", "").upper()
    direction = parsed.get("DIRECTION", "").capitalize()
    entry_price = parsed.get("ENTRY_PRICE", "").replace(",", "")

    if not symbol or not entry_price:
        return []

    return [{
        "type": "photo_signal",
        "symbol": symbol,
        "direction": direction if direction in ("Long", "Short") else None,
        "entry_price": entry_price,
    }]


def parse_row(row):
    text = row.get("text", "") or ""
    results = []
    results.extend(extract_whale_alert(text))
    results.extend(extract_symbol_block(text))

    if not results and row.get("has_photo", "").lower() == "true":
        results.extend(extract_from_photo(row.get("photo_path", "") or ""))

    for r in results:
        r["message_id"] = row.get("id", "")
        r["date"] = row.get("date", "")
        r["has_photo"] = row.get("has_photo", "")
        r["photo_path"] = row.get("photo_path", "")
    return results


def main(input_path="messages.csv", output_path="extracted.csv"):
    seen_ids = set()
    all_results = []
    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            msg_id = row.get("id", "")
            if msg_id in seen_ids:
                continue
            seen_ids.add(msg_id)
            all_results.extend(parse_row(row))

    fieldnames = ["message_id", "date", "type", "symbol", "direction", "entry_price", "has_photo", "photo_path"]
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
