import pandas as pd
import os
import json
import logging
from sqlalchemy import create_engine, text
from typing import List, Dict

logger = logging.getLogger(__name__)


class DataPipeline:
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        os.makedirs("data", exist_ok=True)

    def process(self, raw_data: List[Dict]) -> pd.DataFrame:
        df = pd.DataFrame(raw_data)
        if df.empty:
            return df

        # Clean
        df["text"] = df["text"].str.strip()
        df["author"] = df["author"].str.strip()
        df["word_count"] = df["text"].str.split().str.len()
        df["char_count"] = df["text"].str.len()

        # Remove duplicates
        before = len(df)
        df = df.drop_duplicates(subset=["text"])
        logger.info(f"Removed {before - len(df)} duplicates")

        # Add metadata
        df["scraped_at"] = pd.Timestamp.now().isoformat()
        df = df.sort_values("author").reset_index(drop=True)

        return df

    def save_to_csv(self, df: pd.DataFrame, path: str) -> None:
        df.to_csv(path, index=False)
        logger.info(f"Saved CSV: {path} ({len(df)} rows)")

    def save_to_json(self, df: pd.DataFrame, path: str) -> None:
        records = df.to_dict(orient="records")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved JSON: {path} ({len(records)} records)")

    def save_to_db(self, df: pd.DataFrame) -> None:
        if not self.db_url:
            logger.warning("No DATABASE_URL set — skipping DB save")
            return

        try:
            engine = create_engine(self.db_url)
            with engine.connect() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS quotes (
                        id SERIAL PRIMARY KEY,
                        text TEXT NOT NULL,
                        author TEXT NOT NULL,
                        tags TEXT,
                        tag_count INTEGER,
                        page INTEGER,
                        word_count INTEGER,
                        char_count INTEGER,
                        scraped_at TEXT
                    )
                """))
                conn.commit()

            df.to_sql("quotes", engine, if_exists="replace", index=False)
            logger.info(f"Saved {len(df)} rows to database table 'quotes'")
        except Exception as e:
            logger.error(f"DB save failed: {e}")

    def get_stats(self, df: pd.DataFrame) -> Dict:
        return {
            "total": len(df),
            "unique_authors": df["author"].nunique(),
            "avg_word_count": round(df["word_count"].mean(), 1),
            "most_quoted": df["author"].value_counts().head(5).to_dict(),
            "top_tags": df["tags"].str.split(", ").explode().value_counts().head(10).to_dict(),
        }
