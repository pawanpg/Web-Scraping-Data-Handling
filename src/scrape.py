import argparse, time, pathlib, requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DataBot/1.0)"}

def fetch(url, retries=3, backoff=1.5):
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r.text
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(backoff ** i)

def parse_table(html: str,url):
    soup = BeautifulSoup(html, "html.parser")
    # Try read_html first
    try:
        dfs = pd.read_html(url)
        if dfs:
            return dfs[0]
    except Exception:
        pass
    # Fallback: manual parsing for a table with <table>
    table = soup.find("table")
    if not table:
        return pd.DataFrame()
    rows = []
    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td","th"])]
        if cells:
            rows.append(cells)
    df = pd.DataFrame(rows[1:], columns=headers if headers and len(headers)==len(rows[0]) else None)
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--output", type=pathlib.Path, default=pathlib.Path("outputs/data.csv"))
    args = ap.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)

    url = args.url
    output_file = args.output
    print(f"Scraping {url} -> saving to {output_file}")

    html = fetch(args.url)
    url = args.url
    df = parse_table(html,url)
    if df.empty:
        print("No table found. Saving raw HTML for debugging at outputs/page.html")
        (args.output.parent / "page.html").write_text(html, encoding="utf-8")
    else:
        df.to_csv(args.output, index=False)
        try:
            df.to_parquet(args.output.with_suffix(".parquet"), index=False)
        except Exception:
            pass
        print(f"Saved {len(df)} rows to {args.output}")

if __name__ == "__main__":
    main()
