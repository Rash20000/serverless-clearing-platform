#!/usr/bin/env python3
"""Synthetic Clearing Feed Generator.

Generates realistic fixed-width clearing feed flat files conforming to FIELD_LAYOUT:
  - id:          bytes 0..10   (Account/CUSIP identifier, e.g. 'ACC0000001')
  - entity_name: bytes 10..40  (Counterparty/Asset name, e.g. 'ALPHA_INDEX_FUND')
  - status:      bytes 40..50  (Trading status, e.g. 'SETTLED', 'ACTIVE')
  - amount:      bytes 50..62  (Transaction amount in cents, e.g. '000000150000' = $1,500.00)
Total row width: 62 bytes + delimiter (1 or 2 bytes).

Includes trailer record with control totals (count and amount sum) for two-phase reconciliation.
Supports error injection to test pipeline tolerance thresholds (<= 10 warning, > 10 failure).
"""

from __future__ import annotations

import argparse
import random
import sys

ENTITIES = [
    "ALPHA_TOTAL_MARKET_FUND",
    "BETA_500_INDEX_FUND",
    "GAMMA_GLOBAL_ALLOCATION",
    "DELTA_SPDR_INDEX_TRUST",
    "EPSILON_DIVIDEND_EQUITY",
    "ZETA_PRIME_CLEARING",
    "ETA_CAPITAL_SETTLEMENT",
    "THETA_SECURITIES_PRIME",
    "IOTA_GLOBAL_CUSTODY",
    "KAPPA_GLOBAL_MARKETS",
]

STATUSES = ["ACTIVE", "SETTLED", "CLEARED", "PENDING"]


def format_row(record_id: str, entity: str, status: str, amount_cents: int) -> str:
    """Format row strictly according to 62-byte FIELD_LAYOUT."""
    col_id = f"{record_id[:10]:<10}"
    col_entity = f"{entity[:30]:<30}"
    col_status = f"{status[:10]:<10}"
    col_amount = f"{amount_cents:012d}"[:12]
    return f"{col_id}{col_entity}{col_status}{col_amount}"


def generate_feed(
    num_records: int = 5000,
    inject_errors: int = 0,
    line_ending: str = "\n",
) -> str:
    lines = []
    total_amount_cents = 0

    # 1. Header record (62 bytes)
    header = format_row("HDR0000001", "CLEARING_BATCH_DAILY", "HEADER", 0)
    lines.append(header)

    # 2. Body records
    for i in range(1, num_records + 1):
        rec_id = f"ACC{i:07d}"
        entity = random.choice(ENTITIES)
        status = random.choice(STATUSES)
        cents = random.randint(1000, 500000)  # $10.00 to $5,000.00

        # Inject malformed rows if requested (blank ID produces unparseable record)
        if inject_errors > 0 and i <= inject_errors:
            lines.append(format_row("          ", "CORRUPTED_MALFORMED_RECORD", "INVALID", 0))
        else:
            lines.append(format_row(rec_id, entity, status, cents))
            total_amount_cents += cents

    # 3. Trailer record (62 bytes) with control totals
    trailer = format_row(
        "TRL0000001",
        f"CONTROL_COUNT={num_records}",
        "TRAILER",
        total_amount_cents,
    )
    lines.append(trailer)

    return line_ending.join(lines) + line_ending


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic financial clearing flat feed.")
    parser.add_argument("-n", "--num-records", type=int, default=5000, help="Number of records to generate (default: 5000)")
    parser.add_argument("-e", "--inject-errors", type=int, default=0, help="Number of corrupted rows to inject (default: 0)")
    parser.add_argument("-o", "--output", type=str, default="", help="Output filepath (default: stdout)")
    parser.add_argument("--crlf", action="store_true", help="Use CRLF delimiters instead of LF")

    args = parser.parse_args()
    delimiter = "\r\n" if args.crlf else "\n"
    content = generate_feed(args.num_records, args.inject_errors, delimiter)

    if args.output:
        with open(args.output, "w", encoding="latin-1", newline="") as f:
            f.write(content)
        print(f"Generated clearing feed: {args.output} ({len(content)} bytes, {args.num_records} records, {args.inject_errors} injected errors)")
    else:
        sys.stdout.write(content)


if __name__ == "__main__":
    main()
