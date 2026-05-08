import pytest
import pandas as pd
from scraper.pipeline import DataPipeline


def test_process_cleans_data():
    raw = [
        {"text": "  Hello world  ", "author": "  Author One  ", "tags": "love, life", "tag_count": 2, "page": 1},
        {"text": "Another quote", "author": "Author Two", "tags": "wisdom", "tag_count": 1, "page": 1},
    ]
    pipeline = DataPipeline()
    df = pipeline.process(raw)
    assert len(df) == 2
    assert df.iloc[0]["text"] == "Hello world"
    assert "word_count" in df.columns
    assert "char_count" in df.columns
    assert "scraped_at" in df.columns


def test_process_removes_duplicates():
    raw = [
        {"text": "Same quote", "author": "Author", "tags": "", "tag_count": 0, "page": 1},
        {"text": "Same quote", "author": "Author", "tags": "", "tag_count": 0, "page": 2},
    ]
    pipeline = DataPipeline()
    df = pipeline.process(raw)
    assert len(df) == 1


def test_word_count():
    raw = [{"text": "one two three", "author": "A", "tags": "", "tag_count": 0, "page": 1}]
    pipeline = DataPipeline()
    df = pipeline.process(raw)
    assert df.iloc[0]["word_count"] == 3


def test_empty_data():
    pipeline = DataPipeline()
    df = pipeline.process([])
    assert df.empty


def test_stats():
    raw = [
        {"text": "Quote one", "author": "Alice", "tags": "love", "tag_count": 1, "page": 1},
        {"text": "Quote two", "author": "Bob", "tags": "life", "tag_count": 1, "page": 1},
        {"text": "Quote three", "author": "Alice", "tags": "love", "tag_count": 1, "page": 1},
    ]
    pipeline = DataPipeline()
    df = pipeline.process(raw)
    stats = pipeline.get_stats(df)
    assert stats["total"] == 3
    assert stats["unique_authors"] == 2
    assert "Alice" in stats["most_quoted"]
