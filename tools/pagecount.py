"""Measure how many pages the written answer takes, without a human in the loop.

    python tools/pagecount.py

Renders two PDFs with headless Chrome and counts their pages:

  full           the whole document
  written-only   answer.md truncated at the Required Submission Packet divider

The second number is the one the brief caps at 4. Counting pages by asking
someone to read a print dialog is not a measurement loop, it is a bottleneck.

Chrome headless honours the same @page rules as the print dialog set to
"Default", so these counts track what you would get from Ctrl+P.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from render import render, inline, CSS, MERMAID  # noqa: E402

CHROME = Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe")
ROOT = Path(__file__).resolve().parents[1]
DIVIDER = "# Operating artifact"


def build_html(md: str, title: str) -> str:
    lines = md.split("\n")
    heading = lines[0].lstrip("# ").strip()
    meta_end = next(i for i, l in enumerate(lines) if l.strip() == "---")
    meta = [l for l in lines[1:meta_end] if l.strip()]
    body = render("\n".join(lines[meta_end + 1:]))
    subtitle = "<br>".join(inline(m) for m in meta)
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8">'
            f"<title>{title}</title><style>{CSS}</style></head><body>"
            f"<h1>{heading}</h1><div class=\"subtitle\">{subtitle}</div>"
            f"{body}{MERMAID}</body></html>")


def pdf_pages(pdf: Path) -> int:
    raw = pdf.read_bytes()
    counts = [int(m) for m in re.findall(rb"/Count\s+(\d+)", raw)]
    if counts:
        return max(counts)
    return len(re.findall(rb"/Type\s*/Page[^s]", raw))


def measure(html: str, tmp: Path, label: str) -> int:
    src = tmp / f"{label}.html"
    out = tmp / f"{label}.pdf"
    src.write_text(html, encoding="utf-8")
    subprocess.run(
        [str(CHROME), "--headless=new", "--disable-gpu", "--no-sandbox",
         "--run-all-compositor-stages-before-draw",
         "--virtual-time-budget=8000",          # let Mermaid finish rendering
         "--no-pdf-header-footer",
         f"--print-to-pdf={out}", src.as_uri()],
        capture_output=True, timeout=120,
    )
    return pdf_pages(out) if out.exists() else -1


def main() -> int:
    md = (ROOT / "submission" / "answer.md").read_text(encoding="utf-8")
    written_only = md.split(DIVIDER)[0].rstrip().rstrip("-").rstrip()

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        full = measure(build_html(md, "full"), tmp, "full")
        wa = measure(build_html(written_only, "written"), tmp, "written")

    def words(text: str) -> int:
        text = re.sub(r"<!--.*?-->", "", text, flags=re.S)
        text = re.sub(r"```mermaid.*?```", "", text, flags=re.S)
        text = re.sub(r"(?m)^\|.*$", "", text)
        return len([w for w in text.split() if re.search(r"\w", w)])

    wa_words = words(written_only)
    print(f"written answer : {wa} pages   ({wa_words:,} prose words)   LIMIT 4")
    print(f"full document  : {full} pages")
    print(f"packet section : {full - wa} pages")
    print()
    if wa <= 4:
        print("PASS - written answer is within the brief's 4-page limit.")
        return 0
    over = wa - 4
    # Rough words-per-page from what is actually on the page right now.
    per_page = wa_words / wa if wa else 0
    print(f"OVER by {over} page(s). Roughly {per_page * over:,.0f} words need to go,")
    print("or the equivalent in table rows and figure height.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
