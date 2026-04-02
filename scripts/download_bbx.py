import requests
from datetime import datetime, UTC
from pathlib import Path

URL = "https://admin.bbr.com/download/product-bbx.csv"

def main():
    Path("data").mkdir(exist_ok=True)

    filename = f"data/product-bbx_{datetime.now(UTC).date()}.csv"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "Accept": "*/*",
        "Referer": "https://www.bbr.com/",
    }

    r = requests.get(URL, headers=headers, timeout=60, allow_redirects=True)

    print("HTTP status:", r.status_code)
    print("Final URL:", r.url)
    print("Content-Type:", r.headers.get("Content-Type"))

    if r.status_code != 200:
        raise RuntimeError(f"Download failed with status {r.status_code}")

    if '"Property","Vintage"' not in r.text[:500]:
        raise RuntimeError("Response does not look like BBX CSV")

    with open(filename, "wb") as f:
        f.write(r.content)

    print("Saved", filename)

if __name__ == "__main__":
    main()
