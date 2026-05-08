# Web Scraper + Data Pipeline

Async web scraper with data cleaning pipeline. Scrapes quotes, cleans with Pandas, exports to CSV/JSON/PostgreSQL.

> Python 3.11 · BeautifulSoup4 · Pandas · SQLAlchemy · PostgreSQL · pytest · Docker

## Features
- Multi-page scraping with rate limiting
- Data cleaning: deduplication, trimming, word count
- Export to CSV, JSON, and PostgreSQL
- Scheduled runs every 6 hours
- Statistics: top authors, top tags, averages
- 5 pytest unit tests

## Quick start
```bash
pip install -r requirements.txt
python main.py
```

## With scheduler (every 6 hours)
```bash
python scheduler.py
```

## With Docker + PostgreSQL
```bash
docker-compose up --build
```

## Run tests
```bash
pytest tests/ -v
```

## Output files
- `data/quotes.csv` — spreadsheet format
- `data/quotes.json` — JSON format
- PostgreSQL table `quotes` (if DB configured)

## Project structure
```
scraper/
├── spider.py      ← HTTP requests + HTML parsing
└── pipeline.py    ← Pandas cleaning + export
main.py            ← Run once
scheduler.py       ← Run every 6h
tests/             ← pytest tests
```

## License
MIT — Kenza Nabaghi
