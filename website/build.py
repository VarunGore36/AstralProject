#!/usr/bin/env python3
"""Build website/docs/*.html from docs/*.md so the site stands alone.

Usage:  python3 website/build.py
Reads ../docs/*.md, writes docs/<name>.html with the site chrome.
Re-run after editing any doc. No dependencies beyond stdlib + markdown.
"""

import pathlib
import markdown

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = ROOT / "docs"
OUT = ROOT / "website" / "docs"

# (filename, nav label, page title, description)
RESEARCH = [
    ("001-taker-count-vs-volume.md", "Note 001", "Taker count is not volume",
     "15,655 prints: count screams, volume shrugs."),
    ("002-dust-plus-whales.md", "Note 002", "Dust plus whales",
     "Median print $8; top 1% carries 46% of volume."),
]

# (filename, nav label, page title, description)
PAGES = [
    ("normalized-schema.md", "Schema", "Normalized event schema v1",
     "The Parquet format the normalizer targets."),
    ("replay-design.md", "Replay", "Replay engine design v1",
     "The contract the deterministic engine satisfies."),
    ("cross-machine-repro.md", "Repro", "Cross-machine reproduction procedure",
     "How a second machine reproduces a result."),
    ("soak-runbook.md", "Soak", "72-hour soak runbook",
     "Sizing, shakedown, supervision, judging."),
    ("execution-model.md", "Fills", "Execution model v1",
     "When a resting order counts as filled."),
    ("audit-2026-10-06.md", "Audit", "Pipeline audit 2026-10-06",
     "145 tests, 3 fixes, 13 ranked findings."),
    ("failure-policy.md", "Policy", "Failure policy",
     "What each subcommand does with bad data."),
    ("benchmark-harness.md", "Bench", "Benchmark harness v1",
     "One capture plus one config into a hashed report."),
    ("status-2026-10-10.md", "Status", "Project status 2026-10-10",
     "Numbers, flowcharts, gates, and what ends the OSS."),
]

TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title} — Astral Project</title>
  <meta name="description" content="{desc}" />
  <meta name="theme-color" content="#05070c" />
  <link rel="icon" type="image/png" href="../favicon.png" />
  <meta property="og:image" content="https://astral-project-ruddy.vercel.app/logo.png" />
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet" />
  <link rel="stylesheet" href="../styles.css" />
  <link rel="stylesheet" href="../docs.css" />
</head>
<body>
  <a class="skip-link" href="#main">Skip to content</a>

  <header class="site-header scrolled" id="top">
    <div class="wrap header-inner">
      <a class="brand" href="../index.html" aria-label="Astral Project home">
        <img class="brand-mark brand-logo" src="../logo.png" alt="Astral Project logo" width="26" height="26" />
        <span class="brand-name">Astral <span class="brand-sub">Project</span></span>
      </a>
      <nav class="site-nav doc-nav" aria-label="Docs">
{navlinks}
      </nav>
      <a class="btn btn-ghost btn-small header-cta" href="https://github.com/VarunGore36/AstralProject/blob/main/{srcdir}/{src}" target="_blank" rel="noopener">Source .md</a>
    </div>
  </header>

  <main id="main">
    <section class="section doc-page" aria-labelledby="doc-title">
      <div class="wrap narrow">
        <p class="eyebrow reveal"><a href="{crumb_href}">← {crumb_label}</a></p>
        <article class="doc-prose">
{body}
        </article>
        <div class="oss-cta reveal">
          <a class="btn btn-ghost" href="../index.html">← Back to site</a>
          <a class="btn btn-ghost" href="https://github.com/VarunGore36/AstralProject/blob/main/{srcdir}/{src}" target="_blank" rel="noopener">Source .md on GitHub</a>
        </div>
      </div>
    </section>
  </main>

  <footer class="site-footer">
    <div class="wrap footer-inner">
      <div class="footer-brand">
        <span class="brand-name">Astral Project</span>
        <p>Open, reproducible measurement for crypto markets. Apache-2.0.</p>
      </div>
    </div>
    <div class="wrap footer-base">
      <p>Historical research and simulation only. No execution, no trading, no capital — and no claims about returns.</p>
    </div>
  </footer>

  <script src="../script.js" defer></script>
</body>
</html>
"""


def build() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    ROUT = ROOT / "website" / "research"
    ROUT.mkdir(parents=True, exist_ok=True)
    navlinks = "\n".join(
        f'        <a href="{name[:-3]}.html">{label}</a>' for name, label, _, _ in PAGES
    )
    for name, _, title, desc in PAGES:
        text = (SRC / name).read_text()
        body = markdown.markdown(
            text, extensions=["fenced_code", "tables", "toc", "sane_lists"]
        )
        page = TEMPLATE.format(
            title=title,
            desc=desc,
            navlinks=navlinks,
            body=body,
            src=name,
            srcdir="docs",
            crumb_href="../index.html#opensource",
            crumb_label="Docs",
        )
        (OUT / (pathlib.Path(name).stem + ".html")).write_text(page)
        print(f"wrote website/docs/{pathlib.Path(name).stem}.html")
    for name, _, title, desc in RESEARCH:
        text = (ROOT / "research" / name).read_text()
        body = markdown.markdown(
            text, extensions=["fenced_code", "tables", "toc", "sane_lists"]
        )
        page = TEMPLATE.format(
            title=title,
            desc=desc,
            navlinks=navlinks,
            body=body,
            src=name,
            srcdir="research",
            crumb_href="../index.html#research",
            crumb_label="Research",
        )
        (ROUT / (pathlib.Path(name).stem + ".html")).write_text(page)
        print(f"wrote website/research/{pathlib.Path(name).stem}.html")


if __name__ == "__main__":
    build()
