# Web Scraping & Data Handling (Requests + BeautifulSoup + Pandas)

A simple, robust starter for scraping tabular data from web pages and saving to CSV/Parquet.

## Features
- Fetch pages with retry & polite headers
- Parse with BeautifulSoup
- Normalize and export to CSV/Parquet with Pandas
- CLI + Jupyter notebook

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python src/scrape.py --url "https://example.com" --out outputs/data.csv
```
