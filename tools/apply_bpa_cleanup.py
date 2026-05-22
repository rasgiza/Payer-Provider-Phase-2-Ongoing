"""apply_bpa_cleanup.py — fix BPA violations in PayerAnalytics + ProviderAnalytics
semantic models.

Two cleanups:

1. ``summarizeBy: sum`` → ``summarizeBy: none`` on non-aggregatable numeric
   columns (surrogate keys, flags, scores, age, year/month parts, ZIP, NPI).
   Summing these is meaningless and triggers BPA rule "Set IsHidden /
   summarizeBy correctly on non-aggregatable columns".

2. Add ``formatString: "\\$#,0;(\\$#,0);\\$0"`` to currency / dollar measures
   that currently have only ``annotation PBI_FormatHint = {"isGeneralNumber":true}``
   and no explicit ``formatString``.

Idempotent — re-running adds nothing.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# (1) Columns whose summarizeBy must be 'none'
# ---------------------------------------------------------------------------
# Regex patterns matching column names. Anything matched here will have
# `summarizeBy: sum` rewritten to `summarizeBy: none`.
NON_AGG_NAME_PATTERNS = [
    re.compile(r".*_key$"),           # surrogate join keys
    re.compile(r".*_id$"),            # business IDs
    re.compile(r".*_flag$"),          # boolean flags
    re.compile(r"^is_.*"),
    re.compile(r"^has_.*"),
    re.compile(r".*_score$"),         # risk / quality scores
    re.compile(r"^age$"),
    re.compile(r"^year$"),
    re.compile(r"^quarter$"),
    re.compile(r"^month_number$"),
    re.compile(r"^month_name$"),
    re.compile(r"^day_of_.*"),
    re.compile(r"^day_name$"),
    re.compile(r"^week_of_year$"),
    re.compile(r"^fiscal_(year|quarter)$"),
    re.compile(r"^measurement_year$"),
    re.compile(r"^year_month$"),
    re.compile(r"^zip_code$"),
    re.compile(r"^npi_number$"),
    re.compile(r"^rxnorm_code$"),
    re.compile(r"^icd_code$"),
    re.compile(r"^drg_code$"),
    re.compile(r"^date_key$"),
    re.compile(r".*_date_key$"),
]


def is_non_agg(name: str) -> bool:
    n = name.lower().strip()
    return any(p.match(n) for p in NON_AGG_NAME_PATTERNS)


COLUMN_DECL = re.compile(r"^(\s*)column\s+([A-Za-z_][\w]*)\s*$")
SUMMARIZE_LINE = re.compile(r"^(\s*)summarizeBy:\s*sum\s*$")


def fix_summarize_by(text: str) -> tuple[str, int]:
    lines = text.splitlines(keepends=False)
    out: list[str] = []
    pending_col: str | None = None
    pending_indent = 0
    fixes = 0
    for line in lines:
        m = COLUMN_DECL.match(line)
        if m:
            pending_col = m.group(2)
            pending_indent = len(m.group(1))
            out.append(line)
            continue
        if pending_col is not None:
            sm = SUMMARIZE_LINE.match(line)
            if sm and is_non_agg(pending_col):
                out.append(f"{sm.group(1)}summarizeBy: none")
                fixes += 1
                pending_col = None
                continue
            # End of the column block? blank line or a new top-level decl.
            if line.strip() == "" or COLUMN_DECL.match(line) or re.match(r"^\s*(measure|table)\s", line):
                pending_col = None
        out.append(line)
    return "\n".join(out) + ("\n" if text.endswith("\n") else ""), fixes


# ---------------------------------------------------------------------------
# (2) Currency formatString on dollar measures
# ---------------------------------------------------------------------------
# Measures whose names imply dollars; safe to apply currency format.
CURRENCY_MEASURE_PATTERN = re.compile(
    r"\b("
    r"total[ _-]?(billed|paid|cost|charges|amount|revenue|recovery|premium|appeal[ _-]?recovery|payer[ _-]?cost|patient[ _-]?cost|medication[ _-]?cost)"
    r"|at[ _-]?risk[ _-]?revenue"
    r"|appeal[ _-]?recovery"
    r"|medical[ _-]?paid"
    r"|premium[ _-]?collected"
    r"|paid[ _-]?by[ _-]?payment[ _-]?date"
    r"|avg[ _-]?(charges|cost|amount|amount[ _-]?recovered|expected[ _-]?reimbursement)"
    r"|margin[ _-]?per[ _-]?case"
    r")\b",
    re.IGNORECASE,
)

CURRENCY_FORMAT = 'formatString: "\\$#,0;(\\$#,0);\\$0"'

MEASURE_DECL = re.compile(r"^(\s*)measure\s+('[^']+'|[A-Za-z_][\w]*)\s*=")


def add_currency_format(text: str) -> tuple[str, int]:
    """Insert ``formatString`` on currency measures missing one."""
    lines = text.splitlines(keepends=False)
    out: list[str] = []
    fixes = 0
    i = 0
    while i < len(lines):
        line = lines[i]
        m = MEASURE_DECL.match(line)
        if not m:
            out.append(line)
            i += 1
            continue

        measure_name = m.group(2).strip("'")
        indent = m.group(1)

        # Collect the measure block: from this line up to (but not including)
        # the next blank-line followed by a non-indented header, OR the next
        # `measure`/`column`/`table` declaration. Easier: read lines until
        # indentation drops below `indent + one tab`.
        block_start = i
        i += 1
        while i < len(lines):
            nxt = lines[i]
            if nxt.strip() == "":
                i += 1
                continue
            # Stop if a sibling declaration at <= measure indent appears.
            if re.match(rf"^{re.escape(indent)}(measure|column)\s", nxt):
                break
            if re.match(r"^\S", nxt):  # table-level decl at column 0
                break
            i += 1
        block_end = i  # exclusive

        block = lines[block_start:block_end]

        if CURRENCY_MEASURE_PATTERN.search(measure_name):
            has_format = any(re.match(r"^\s*formatString\s*:", b) for b in block)
            has_general = any('isGeneralNumber":true' in b for b in block)
            if not has_format and has_general:
                # Insert formatString right after the declaration line.
                inner_indent = indent + "\t"
                # Find where the declaration "ends" — for multi-line DAX with =
                # VAR ... RETURN ..., the declaration spans until first
                # `\t\t<keyword>:` line.  Easiest: insert before the first
                # indented property line.
                insert_at = 1  # after the declaration line within block
                while insert_at < len(block):
                    stripped = block[insert_at].lstrip()
                    if stripped == "":
                        insert_at += 1
                        continue
                    # First property-style line we've encountered
                    break
                block.insert(insert_at, f"{inner_indent}{CURRENCY_FORMAT}")
                fixes += 1

        out.extend(block)
    return "\n".join(out) + ("\n" if text.endswith("\n") else ""), fixes


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def process_file(path: Path) -> tuple[int, int]:
    text = path.read_text(encoding="utf-8")
    text2, s_fix = fix_summarize_by(text)
    text3, c_fix = add_currency_format(text2)
    if s_fix or c_fix:
        path.write_text(text3, encoding="utf-8")
    return s_fix, c_fix


def main(argv: list[str]) -> int:
    if argv:
        roots = [Path(p) for p in argv]
    else:
        here = Path(__file__).resolve().parent.parent
        roots = [
            here / "workspace" / "PayerAnalytics.SemanticModel" / "definition" / "tables",
            here / "workspace" / "ProviderAnalytics.SemanticModel" / "definition" / "tables",
        ]
    total_s = total_c = 0
    for root in roots:
        if not root.exists():
            print(f"[skip] {root}")
            continue
        for tmdl in sorted(root.glob("*.tmdl")):
            s, c = process_file(tmdl)
            total_s += s
            total_c += c
            mark = "+" if (s or c) else "="
            print(f"{mark} {tmdl.relative_to(root.parent.parent.parent)}: summarizeBy={s} currency={c}")
    print(f"\nDone. summarizeBy fixes: {total_s}   currency formatString inserts: {total_c}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
