"""Sync HealthcareHLSAgent system prompt from system_prompt.md
into both stage_config.json files (draft + published).

The .md file is the human-editable source of truth.
This script flattens it into the aiInstructions JSON string that
fabric-cicd reads when deploying the DataAgent.

Usage:
  python scripts/sync_hls_prompt.py
"""
from pathlib import Path
import json
import re

REPO = Path(__file__).resolve().parent.parent
AGENT = REPO / "workspace" / "HealthcareHLSAgent.DataAgent"
MD = AGENT / "system_prompt.md"
DRAFT = AGENT / "Files" / "Config" / "draft" / "stage_config.json"
PUBLISHED = AGENT / "Files" / "Config" / "published" / "stage_config.json"


def md_to_ai_instructions(md_text: str) -> str:
    """Convert the readable .md into a flat instruction string.

    Strategy: keep all content, strip the leading title block and
    the human-only 'edit this file' note. The fabric-cicd-deployed
    agent will see exactly the same semantics, just flattened.
    """
    # Drop the top H1 + blockquote callout up to the first --- divider
    text = md_text
    parts = text.split("\n---\n", 1)
    if len(parts) == 2:
        text = parts[1].lstrip("\n")
    # Strip markdown code fences but keep the inside content
    # (LLMs read backtick-fenced blocks fine; we just want clean text)
    # Collapse trailing whitespace
    text = re.sub(r"[ \t]+\n", "\n", text)
    # Collapse 3+ blank lines to 2
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip() + "\n"


def update_stage_config(path: Path, ai_instructions: str) -> None:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    cfg["aiInstructions"] = ai_instructions
    path.write_text(
        json.dumps(cfg, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    md_text = MD.read_text(encoding="utf-8")
    ai = md_to_ai_instructions(md_text)
    for path in (DRAFT, PUBLISHED):
        update_stage_config(path, ai)
        print(f"  [OK] {path.relative_to(REPO)} aiInstructions={len(ai)} chars")
    print(f"\nSynced {len(ai)} chars from system_prompt.md to both stage_configs.")


if __name__ == "__main__":
    main()
