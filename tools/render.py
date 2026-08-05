"""Markdown -> styled HTML for the submission PDF.

    python tools/render.py submission/answer.md

Then open the HTML in Chrome and Ctrl+P -> Save as PDF.

WHY A HAND-ROLLED CONVERTER. Three reasons, in order of importance:

1. No word processor ever touches the text. The challenge's own pre-screen
   treats invisible and zero-width Unicode as an automatic reject, and word
   processors inject exactly that - smart quotes, non-breaking spaces, zero
   width joiners. The Markdown stays ASCII-only and typography is applied
   here, at render time, where it cannot travel back into the source.
2. No install. Pandoc would work but needs a dependency the rest of this repo
   does not have, and "standard library only" is a claim the README makes.
3. Mermaid renders. Static converters cannot run JavaScript, so a Mermaid
   diagram would come out as a block of text. Chrome runs it and prints it.

This handles the Markdown subset the answer actually uses. It is not a general
CommonMark implementation and does not pretend to be.
"""

from __future__ import annotations

import html
import re
import sys
from pathlib import Path

# --------------------------------------------------------------------------
# Stylesheet. Four colors: ink, grey, rule, accent.
# --------------------------------------------------------------------------

CSS = """
:root{
  --ink:    #111111;
  --grey:   #5a5a5a;
  --rule:   #d8d8d8;
  --accent: #35506B;
  --panel:  #f7f8f9;
}
/* Print with Margins: None so this rule owns the page box. */
@page { size: letter; margin: 0.62in 0.66in; }
html{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }
body{
  margin: 0 auto; max-width: 7.2in; padding: 0;
  font-family: -apple-system, "Segoe UI", Inter, Helvetica, Arial, sans-serif;
  font-size: 10.2pt; line-height: 1.42; color: var(--ink); background:#fff;
  font-kerning: normal; text-rendering: optimizeLegibility;
}
h1{
  font-family: Georgia, "Iowan Old Style", "Times New Roman", serif;
  font-size: 17pt; font-weight: 600; letter-spacing:-0.01em;
  margin: 0 0 0.3rem; line-height: 1.16;
  border-bottom: 2px solid var(--accent); padding-bottom: 0.35rem;
}
h2{
  font-family: Georgia, "Iowan Old Style", serif;
  font-size: 12.6pt; font-weight: 600; margin: 1.15rem 0 0.2rem;
  padding-bottom: 0.2rem; border-bottom: 1px solid var(--rule);
  break-after: avoid; page-break-after: avoid;
}
h3{
  font-family: Georgia, "Iowan Old Style", serif;
  font-size: 11pt; font-weight: 600; margin: 0.85rem 0 0.22rem;
  break-after: avoid; page-break-after: avoid;
}
h4{
  font-family: Georgia, "Iowan Old Style", serif;
  font-size: 10.2pt; font-weight: 600; font-style: italic;
  margin: 0.7rem 0 0.2rem; color: var(--ink);
  break-after: avoid; page-break-after: avoid;
}
h2 + p, h3 + p, h4 + p, h2 + ul, h3 + ul, h2 + table, h3 + table,
h4 + table, h4 + p{ margin-top: 0.3rem; }
p{ margin: 0 0 0.5rem; orphans: 3; widows: 3; }
strong{ font-weight: 640; }
em{ font-style: italic; }
ul, ol{ margin: 0 0 0.6rem; padding-left: 1.05rem; }
li{ margin-bottom: 0.2rem; }
li::marker{ color: var(--accent); }
.subtitle{ font-size: 8.4pt; color: var(--grey); line-height:1.5; margin-bottom:1rem; }
.subtitle code{ font-size: 7.8pt; }
/* The packet sections are separate deliverables per the brief, so they start
   on a fresh page behind a stated convention rather than padding the count. */
.packet-divider{
  page-break-before: always; break-before: page;
  border-top: 2px solid var(--accent); margin-top: 0; padding-top: 0.5rem;
  font-size: 8.6pt; color: var(--grey); margin-bottom: 1.2rem;
}
.packet-divider strong{ color: var(--ink); }
.lbl{
  font-family: ui-monospace,"SF Mono",Consolas,monospace;
  font-size: 8.3pt; font-weight: 600; color: var(--accent); white-space: nowrap;
}
/* Long tables must be allowed to split across pages. Forcing break-inside:
   avoid on a 15-row table makes the whole block jump to a fresh page and
   strands most of the previous one. Protect individual ROWS instead. */
table{ width:100%; border-collapse: collapse; margin: 0.4rem 0 0.7rem;
       font-size: 8.7pt; break-inside: auto; page-break-inside: auto; }
tr{ break-inside: avoid; page-break-inside: avoid; }
thead{ display: table-header-group; }
th{ text-align:left; font-weight:600; font-size:7.9pt; letter-spacing:0.05em;
    text-transform:uppercase; color:var(--accent);
    border-bottom:1.5px solid var(--accent); padding:0.26rem 0.55rem 0.26rem 0; }
td{ padding:0.26rem 0.55rem 0.26rem 0; border-bottom:1px solid var(--rule);
    vertical-align: top; line-height:1.36; }
th:last-child, td:last-child{ padding-right:0; }
tr:last-child td{ border-bottom:none; }
code{ font-family: ui-monospace,"SF Mono",Consolas,monospace; font-size:8.3pt;
      background:var(--panel); padding:0.04em 0.28em; border-radius:2px; }
pre{ background:var(--panel); border-left:2.5px solid var(--accent);
     padding:0.45rem 0.7rem; margin:0.45rem 0 0.7rem; font-size:8pt;
     line-height:1.4; overflow-x:auto; break-inside:avoid; page-break-inside:avoid; }
pre code{ background:none; padding:0; font-size:inherit; }
hr{ border:none; border-top:1px solid var(--rule); margin:1.1rem 0; }
a{ color:var(--accent); text-decoration:none; border-bottom:1px solid var(--rule); }
figure{ margin:1.2rem 0 1.4rem; break-inside:avoid; page-break-inside:avoid; }
.mermaid{ margin:0; padding:0; background:none; }
/* Cap the diagram's height. It scales to full column width by default, which
   on a three-branch flowchart eats most of a page for no extra clarity. */
.mermaid svg{ width:auto !important; max-width:100% !important;
              max-height:2.05in !important; height:auto !important;
              background:transparent !important; display:block; margin:0 auto; }
"""

MERMAID = """
<script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
<script>
mermaid.initialize({
  startOnLoad:true, theme:'base',
  themeVariables:{
    fontFamily:'-apple-system, Segoe UI, Helvetica, Arial, sans-serif',
    fontSize:'15px', primaryColor:'#ffffff', primaryTextColor:'#111111',
    primaryBorderColor:'#111111', lineColor:'#35506B', background:'transparent'
  },
  flowchart:{ curve:'basis', padding:14, nodeSpacing:34, rankSpacing:58, useMaxWidth:false }
});
</script>
"""

LABEL = re.compile(r"\[(Observed|Estimated|Benchmarked|Assumed)\]")
# Excludes `*` so a bold-wrapped URL (**https://...**) does not swallow its own
# closing markers before the bold rule runs.
BARE_URL = re.compile(r"(?<!\()(?<!\")(https?://[^\s<>\)\*]+)")


def inline(text: str) -> str:
    """Inline formatting. Code spans are protected first so nothing rewrites
    their contents - a `[Observed]` inside backticks must stay literal."""
    slots: list[str] = []

    def stash(match):
        slots.append(f"<code>{html.escape(match.group(1))}</code>")
        return f"\x00{len(slots) - 1}\x00"

    text = re.sub(r"`([^`]+)`", stash, text)
    text = html.escape(text)
    text = re.sub(r"\[([^\]]+)\]\((https?://[^\)]+)\)", r'<a href="\2">\1</a>', text)
    text = BARE_URL.sub(r'<a href="\1">\1</a>', text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"(?<![\w*])\*([^*\n]+)\*(?![\w*])", r"<em>\1</em>", text)
    text = LABEL.sub(r'<span class="lbl">[\1]</span>', text)
    # NO em-dash substitution. An earlier version turned " - " into " — " as a
    # typographic nicety; on a submission judged against an AI answer, the em
    # dash is the single most recognisable model tell. Hyphens stay hyphens.
    text = text.replace("->", "&rarr;")

    for i, slot in enumerate(slots):
        text = text.replace(f"\x00{i}\x00", slot)
    return text


def lint(md: str) -> list[str]:
    """Catch source patterns this converter silently mis-renders.

    A wrapped line beginning with `- ` reads as prose to a human and as a new
    list item to any Markdown parser. That produced a spurious bullet on page
    one of this document and another mid-paragraph in "What stays human", so
    it is now a hard check rather than something to spot in a PDF.
    """
    problems: list[str] = []
    in_fence = False
    prev = ""
    for n, line in enumerate(md.split("\n"), 1):
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            prev = line
            continue
        if in_fence:
            prev = line
            continue
        bullet = re.match(r"^(\s*)-\s+", line)
        # Only a bug when the dash sits at the SAME indent as the prose line
        # above it - that is a wrapped sentence, not a new item. A bullet at
        # column 0 after an indented continuation is the next real item.
        same_indent = bool(bullet) and len(bullet.group(1)) == len(prev) - len(prev.lstrip())
        prev_is_prose = (
            prev.strip()
            and not re.match(r"^\s*[-*]\s+", prev)
            and not re.match(r"^\s*\d+\.\s+", prev)
            and not prev.lstrip().startswith(("|", "#", ">"))
        )
        if bullet and same_indent and prev_is_prose:
            problems.append(
                f"line {n}: dash-led line follows prose and will render as a "
                f"list item -> {line.strip()[:60]}"
            )
        prev = line
    return problems


def render(md: str) -> str:
    md = re.sub(r"<!--.*?-->", "", md, flags=re.S)
    lines = md.split("\n")
    out: list[str] = []
    i = 0
    figure_no = 0

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if not stripped:
            i += 1
            continue

        # Fenced blocks: mermaid becomes a figure, everything else a <pre>.
        if stripped.startswith("```"):
            lang = stripped[3:].strip()
            i += 1
            block: list[str] = []
            while i < len(lines) and not lines[i].strip().startswith("```"):
                block.append(lines[i])
                i += 1
            i += 1
            body = "\n".join(block)
            if lang == "mermaid":
                figure_no += 1
                out.append(f'<figure><pre class="mermaid">\n{body}\n</pre></figure>')
            else:
                out.append(f"<pre><code>{html.escape(body)}</code></pre>")
            continue

        if stripped == "---":
            out.append("<hr>")
            i += 1
            continue

        if stripped.startswith("#"):
            level = len(stripped) - len(stripped.lstrip("#"))
            heading = stripped[level:].strip()
            # The packet sections are separate deliverables under the brief's
            # Required Submission Packet, not part of the 4-page written
            # answer. Start them on a fresh page and say so, rather than
            # letting them silently inflate the page count.
            if heading == "Operating artifact":
                out.append(
                    '<div class="packet-divider"><strong>Required Submission '
                    'Packet.</strong> The written answer ends here. The '
                    'sections below are the packet items the brief lists '
                    'separately: operating artifact, artifact access, evidence '
                    'log, number source labels, AI usage disclosure, failure '
                    'modes, and what stays human.</div>')
            # Shift every heading down one level: the document title is the
            # only <h1>. Without this, each `# Section` in the packet renders
            # at title weight and competes with the actual title.
            out.append(f"<h{min(level + 1, 6)}>{inline(heading)}</h{min(level + 1, 6)}>")
            i += 1
            continue

        # Tables: a pipe row followed by a separator row.
        if stripped.startswith("|") and i + 1 < len(lines) \
                and re.match(r"^\s*\|[\s:|-]+\|\s*$", lines[i + 1]):
            header = [c.strip() for c in stripped.strip("|").split("|")]
            i += 2
            rows: list[list[str]] = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                rows.append([c.strip() for c in lines[i].strip().strip("|").split("|")])
                i += 1
            out.append("<table><thead><tr>"
                       + "".join(f"<th>{inline(c)}</th>" for c in header)
                       + "</tr></thead><tbody>")
            for row in rows:
                out.append("<tr>" + "".join(f"<td>{inline(c)}</td>" for c in row) + "</tr>")
            out.append("</tbody></table>")
            continue

        # Lists. Continuation lines are indented under their bullet.
        bullet = re.match(r"^(\s*)([-*]|\d+\.)\s+(.*)$", line)
        if bullet:
            ordered = bool(re.match(r"^\d+\.$", bullet.group(2)))
            tag = "ol" if ordered else "ul"
            items: list[str] = []
            while i < len(lines):
                m = re.match(r"^(\s*)([-*]|\d+\.)\s+(.*)$", lines[i])
                if m:
                    items.append(m.group(3))
                    i += 1
                elif lines[i].startswith(("  ", "\t")) and lines[i].strip() and items:
                    items[-1] += " " + lines[i].strip()
                    i += 1
                else:
                    break
            out.append(f"<{tag}>" + "".join(f"<li>{inline(x)}</li>" for x in items)
                       + f"</{tag}>")
            continue

        # Paragraph: consume until a blank line or a block-level marker.
        para: list[str] = []
        while i < len(lines) and lines[i].strip() \
                and not lines[i].strip().startswith(("#", "|", "```", "---")) \
                and not re.match(r"^\s*([-*]|\d+\.)\s+", lines[i]):
            para.append(lines[i].strip())
            i += 1
        if para:
            out.append(f"<p>{inline(' '.join(para))}</p>")

    return "\n".join(out)


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__.strip().split("\n")[2])
        return 2
    src = Path(argv[1])
    md = src.read_text(encoding="utf-8")

    for problem in lint(md):
        print(f"  LINT: {problem}")

    # The header block: title, then metadata lines, then the page-count note.
    lines = md.split("\n")
    title = lines[0].lstrip("# ").strip()
    meta_end = next(i for i, l in enumerate(lines) if l.strip() == "---")
    meta = [l for l in lines[1:meta_end] if l.strip()]
    rest = "\n".join(lines[meta_end + 1:])

    subtitle = "<br>".join(inline(m) for m in meta)
    body = render(rest)

    doc = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>{html.escape(title)}</title>
<style>{CSS}</style></head><body>
<h1>{html.escape(title)}</h1>
<div class="subtitle">{subtitle}</div>
{body}
{MERMAID}
</body></html>
"""
    dst = src.with_suffix(".html")
    dst.write_text(doc, encoding="utf-8", newline="\n")
    print(f"wrote {dst}")
    print(f"  {len(doc):,} bytes")
    print("  open in Chrome, then Ctrl+P -> Save as PDF")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
