"""Extract reference-page body content + page-specific search wiring into fragments/<slug>.html.

Run from the site root: python scripts/extract-fragments.py
"""
import re
from pathlib import Path

PAGES = [
    ("fire-and-rescue.html", "fire-and-rescue-nsw"),
    ("ambulance.html", "nsw-ambulance"),
    ("rural-fire-service.html", "nsw-rural-fire-service"),
]


def extract_balanced(text: str, start: int) -> int:
    """Return index just after the matching close for the bracket at text[start]."""
    open_ch = text[start]
    close_ch = {"(": ")", "{": "}", "[": "]"}[open_ch]
    depth = 0
    in_str = None
    i = start
    BACKSLASH = chr(92)
    while i < len(text):
        c = text[i]
        if in_str is not None:
            if c == BACKSLASH:
                i += 2
                continue
            if c == in_str:
                in_str = None
        else:
            if c in ('"', "'", "`"):
                in_str = c
            elif c == open_ch:
                depth += 1
            elif c == close_ch:
                depth -= 1
                if depth == 0:
                    return i + 1
        i += 1
    return i


def extract_dcl_blocks(script_text: str):
    blocks = []
    pattern = re.compile(r"document\.addEventListener\(\s*[\"']DOMContentLoaded[\"']")
    for m in pattern.finditer(script_text):
        paren = script_text.find("(", m.start())
        end = extract_balanced(script_text, paren)
        if end < len(script_text) and script_text[end] == ";":
            end += 1
        blocks.append(script_text[m.start():end])
    return blocks


def main():
    for src_name, slug in PAGES:
        text = Path(src_name).read_text(encoding="utf-8")
        main_m = re.search(r'<main class="main">(.*?)</main>', text, re.DOTALL)
        inner = main_m.group(1)
        body = re.sub(r'<header class="main-header">.*?</header>', "", inner, count=1, flags=re.DOTALL).strip()
        body = re.sub(r'<div class="main-footer-note">.*?</div>', "", body, flags=re.DOTALL).strip()

        scripts = re.findall(r"<script>(.*?)</script>", text, re.DOTALL)
        keep = []
        for s in scripts:
            for blk in extract_dcl_blocks(s):
                if any(k in blk for k in ["search", "filter", "frnsw", "rfs", "ambulance"]):
                    keep.append(blk)

        fragment = body
        if keep:
            fragment += "\n\n<script>\n" + "\n\n".join(keep) + "\n</script>"

        out = Path(f"fragments/{slug}.html")
        out.write_text(fragment, encoding="utf-8")
        print(f"{slug}: {out.stat().st_size} bytes ({len(keep)} script block(s))")


if __name__ == "__main__":
    main()
