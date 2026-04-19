"""
Parse messages.csv and extract symbol and entry price from:
- Whale Alert text messages (e.g., "** Long ** **ETH** with **6x** leverage, entry price **$2188.07**")
- Symbol info messages (e.g., "`Symbol              BTC\nPrice               $70859.90")
- Photo captions containing the above patterns
"""

import csv
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


def parse_row(row):
    text = row.get("text", "") or ""
    results = []
    results.extend(extract_whale_alert(text))
    results.extend(extract_symbol_block(text))
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
