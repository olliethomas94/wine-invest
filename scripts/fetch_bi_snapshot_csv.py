import os
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.biwine.com/v2"

EMAIL = os.getenv("BI_USERNAME")
PASSWORD = os.getenv("BI_PASSWORD")
SALES_LEDGER = os.getenv("BI_SALES_LEDGER")
PURCHASE_LEDGER = os.getenv("BI_PURCHASE_LEDGER")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


def login():
    print("Logging in...")
    response = requests.post(
        f"{BASE_URL}/user/login",
        json={
            "email": EMAIL,
            "password": PASSWORD,
            "salesLedger": SALES_LEDGER,
            "purchaseLedger": PURCHASE_LEDGER,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["token"]


def fetch_price_list(token):
    print("Fetching BI price list...")
    response = requests.get(
        f"{BASE_URL}/market/bi-price-list",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def main():
    token = login()
    rows = fetch_price_list(token)

    if not rows:
        raise RuntimeError("No BI data returned")

    df = pd.DataFrame(rows)

    today = datetime.utcnow().strftime("%Y-%m-%d")
    output_path = DATA_DIR / f"bi-price-list_{today}.csv"

    df.to_csv(output_path, index=False)

    print(f"Saved {len(df)} rows → {output_path}")


if __name__ == "__main__":
    main()