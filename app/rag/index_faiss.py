from __future__ import annotations

from pathlib import Path

import faiss


def load_or_create(path: Path, dim: int) -> faiss.IndexIDMap2:
    if path.exists():
        index = faiss.read_index(str(path))
        if index.d != dim:
            raise ValueError("FAISS index dimension mismatch")
        if not isinstance(index, faiss.IndexIDMap2):
            index = faiss.IndexIDMap2(index)
        return index
    base_index = faiss.IndexFlatL2(dim)
    return faiss.IndexIDMap2(base_index)


def add_vectors(index: faiss.IndexIDMap2, vectors, ids) -> None:
    index.add_with_ids(vectors, ids)


def save_index(index: faiss.IndexIDMap2, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    faiss.write_index(index, str(path))
