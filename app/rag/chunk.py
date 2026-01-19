from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Chunk:
    heading: str | None
    text: str


def _split_sections(text: str) -> list[tuple[str | None, str]]:
    sections: list[tuple[str | None, str]] = []
    current_heading: str | None = None
    current_lines: list[str] = []

    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            if current_lines:
                sections.append((current_heading, "\n".join(current_lines).strip()))
                current_lines = []
            current_heading = stripped.lstrip("#").strip() or None
        elif stripped:
            current_lines.append(stripped)

    if current_lines:
        sections.append((current_heading, "\n".join(current_lines).strip()))

    if not sections:
        sections.append((None, text.strip()))
    return sections


def chunk_text(text: str, max_tokens: int = 800, overlap: int = 120) -> list[Chunk]:
    chunks: list[Chunk] = []
    for heading, section_text in _split_sections(text):
        words = section_text.split()
        if not words:
            continue
        start = 0
        while start < len(words):
            end = min(len(words), start + max_tokens)
            chunk_words = words[start:end]
            chunk = " ".join(chunk_words).strip()
            if chunk:
                chunks.append(Chunk(heading=heading, text=chunk))
            if end == len(words):
                break
            start = max(0, end - overlap)
    return chunks
