from app.rag.chunk import chunk_text


def test_chunking_overlap():
    text = " ".join([f"word{i}" for i in range(2000)])
    chunks = chunk_text(text, max_tokens=200, overlap=50)
    assert len(chunks) > 1
    first = chunks[0].text.split()
    second = chunks[1].text.split()
    assert first[-50:] == second[:50]


def test_chunking_heading():
    text = "# Heading One\n" + "alpha " * 50 + "\n# Heading Two\n" + "beta " * 50
    chunks = chunk_text(text, max_tokens=40, overlap=10)
    assert any(chunk.heading == "Heading One" for chunk in chunks)
    assert any(chunk.heading == "Heading Two" for chunk in chunks)
