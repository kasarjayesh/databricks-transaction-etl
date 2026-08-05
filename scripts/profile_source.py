"""Profile a source transaction file before designing the pipeline.

Run this against a new daily file to understand its shape and quirks BEFORE
writing or changing transformation rules. Every anomaly it reports should end
up as either a unit test case or a data-quality rule.

Six dimensions are profiled:
  1. Structure      - row count, column names, header order
  2. Distribution   - value counts for low-cardinality columns
  3. Completeness   - empty / whitespace-only values per column
  4. Uniqueness     - duplicate counts for candidate key columns
  5. Format         - pattern "shapes" (digits -> 9, letters -> A)
  6. Order/content  - date component order, hidden characters

Stdlib only, so it runs anywhere without Spark or pandas. At ~100K rows the
whole file fits comfortably in memory; for far larger inputs the same checks
translate directly to Spark (groupBy/count, isNull sums,
approx_count_distinct, regexp_replace shapes, summary()).

Usage:
    python scripts/profile_source.py data/inputDataTest.csv

Findings from the original sample (inputDataTest.csv, 100,001 rows) and the
design decision each one drove:

    Single date shape 99/99/9999, day-first  -> one strict format constant
    7 gender variants (m, f, mAle, ...)      -> normalise before matching
    Age bands 30-30 / 150-59 / 140-49        -> DQ warning, not rejection
    14 empty country values                  -> NULL guards + warning rule
    Postcodes in 11 shapes, incl. A9A 9AA    -> optional 4th regex character
    28,745 non-postcode values (XXX)         -> passthrough is a main path
    Tabs / newlines / CJK text in Notes      -> \\p{C} cleanup, not \\n only
    1 duplicate Transaction_id               -> warning, not a critical failure
    Anomalies spread across most columns     -> bronze stores every column raw
"""

from __future__ import annotations

import collections
import csv
import re
import sys

# Columns with more distinct values than this are summarised by shape instead.
MAX_DISTINCT_TO_LIST = 25

# Columns whose format matters more than their content.
SHAPE_COLUMNS = ["transaction_date", "posting_date", "customer_postcode"]

# Columns that could plausibly identify a row.
CANDIDATE_KEYS = ["record_id", "Transaction_id", "customer_key"]


def load(path: str) -> list[dict]:
    """Read the CSV into memory.

    encoding='utf-8-sig' strips a UTF-8 BOM if present (this source has one);
    newline='' lets the csv module handle newlines embedded in quoted fields.
    """
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def shape_of(value: str) -> str:
    """Collapse a value to its format: digits -> 9, letters -> A.

    Turns an unreadable column of 100K values into a handful of templates,
    which is how format variance (dd/MM/yyyy vs yyyy-MM-dd, spaced vs
    unspaced postcodes) becomes visible at a glance.
    """
    return re.sub(r"[A-Za-z]", "A", re.sub(r"\d", "9", value or ""))


def is_blank(value: str | None) -> bool:
    """Empty, missing, or whitespace-only - CSV cannot distinguish these."""
    return not (value or "").strip()


def report_structure(rows: list[dict]) -> None:
    print(f"\n{'=' * 70}\n1. STRUCTURE\n{'=' * 70}")
    print(f"rows    : {len(rows):,}")
    print(f"columns : {len(rows[0])}")
    for i, col in enumerate(rows[0], start=1):
        print(f"  {i:2d}. {col}")


def report_distributions(rows: list[dict]) -> None:
    print(f"\n{'=' * 70}\n2. VALUE DISTRIBUTIONS (low-cardinality columns)\n{'=' * 70}")
    print("Read the tail, not the head: rare values are what break pipelines.\n")
    for col in rows[0]:
        counts = collections.Counter(r[col] for r in rows)
        if len(counts) > MAX_DISTINCT_TO_LIST:
            print(f"{col}: {len(counts):,} distinct values (see shapes below)")
            continue
        print(f"{col}: {len(counts)} distinct")
        for value, count in counts.most_common():
            pct = 100 * count / len(rows)
            display = repr(value) if is_blank(value) else value
            print(f"    {display:<20} {count:>7,}  {pct:5.2f}%")


def report_completeness(rows: list[dict]) -> None:
    print(f"\n{'=' * 70}\n3. COMPLETENESS\n{'=' * 70}")
    for col in rows[0]:
        blanks = sum(1 for r in rows if is_blank(r[col]))
        if blanks:
            print(f"    {col:<22} {blanks:>7,} blank ({100 * blanks / len(rows):.2f}%)")
    print("    (columns not listed are 100% populated)")


def report_uniqueness(rows: list[dict]) -> None:
    print(f"\n{'=' * 70}\n4. UNIQUENESS OF CANDIDATE KEYS\n{'=' * 70}")
    for col in CANDIDATE_KEYS:
        if col not in rows[0]:
            continue
        distinct = len({r[col] for r in rows})
        dupes = len(rows) - distinct
        verdict = "unique - usable as a key" if dupes == 0 else "NOT unique"
        print(f"    {col:<22} {distinct:>7,} distinct, {dupes:>3,} duplicate  ({verdict})")
        if dupes:
            counts = collections.Counter(r[col] for r in rows)
            repeated = [v for v, c in counts.items() if c > 1]
            print(f"        examples: {repeated[:3]}")


def report_shapes(rows: list[dict]) -> None:
    print(f"\n{'=' * 70}\n5. FORMAT SHAPES (digits -> 9, letters -> A)\n{'=' * 70}")
    for col in SHAPE_COLUMNS:
        if col not in rows[0]:
            continue
        shapes = collections.Counter(shape_of(r[col]) for r in rows)
        print(f"\n{col}: {len(shapes)} distinct shape(s)")
        for shape, count in shapes.most_common(15):
            print(f"    {shape or '(empty)':<14} {count:>7,}")
        if len(shapes) == 1:
            print("    -> single format: a strict parse pattern is safe")


def report_date_order(rows: list[dict]) -> None:
    """Distinguish dd/MM from MM/dd - a silent-corruption risk if guessed."""
    print(f"\n{'=' * 70}\n6. DATE COMPONENT ORDER\n{'=' * 70}")
    for col in ("transaction_date", "posting_date"):
        if col not in rows[0]:
            continue
        first_over_12 = second_over_12 = 0
        for r in rows:
            parts = (r[col] or "").split("/")
            if len(parts) == 3 and parts[0].isdigit() and parts[1].isdigit():
                first_over_12 += int(parts[0]) > 12
                second_over_12 += int(parts[1]) > 12
        if first_over_12 and not second_over_12:
            verdict = "day-first (dd/MM/yyyy)"
        elif second_over_12 and not first_over_12:
            verdict = "month-first (MM/dd/yyyy)"
        else:
            verdict = "AMBIGUOUS - confirm with the data provider"
        print(f"    {col:<22} first>12: {first_over_12:>6,}  "
              f"second>12: {second_over_12:>6,}  -> {verdict}")


def report_hidden_characters(rows: list[dict]) -> None:
    """Control and format characters are invisible until they break something."""
    print(f"\n{'=' * 70}\n7. HIDDEN CHARACTERS IN FREE TEXT\n{'=' * 70}")
    for col in ("Notes", "Txn_Description"):
        if col not in rows[0]:
            continue
        found = collections.Counter()
        non_ascii = 0
        for r in rows:
            value = r[col] or ""
            for ch in value:
                if ord(ch) < 32 or ord(ch) in (0x200B, 0xFEFF):
                    found[repr(ch)] += 1
            if any(ord(ch) > 127 for ch in value):
                non_ascii += 1
        print(f"    {col}: {dict(found) or 'none'};  "
              f"rows with non-ASCII text: {non_ascii:,}")


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else "data/inputDataTest.csv"
    rows = load(path)
    if not rows:
        raise SystemExit(f"No rows found in {path}")

    print(f"\nProfiling: {path}")
    report_structure(rows)
    report_distributions(rows)
    report_completeness(rows)
    report_uniqueness(rows)
    report_shapes(rows)
    report_date_order(rows)
    report_hidden_characters(rows)
    print(f"\n{'=' * 70}\nDone. Turn every anomaly above into a test or a DQ rule.\n")


if __name__ == "__main__":
    main()