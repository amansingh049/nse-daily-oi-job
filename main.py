import requests
import csv
import time
from datetime import datetime

# NSE requires proper headers + cookies to avoid 403
BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Connection": "keep-alive"
}

session = requests.Session()
session.headers.update(BASE_HEADERS)


# -----------------------------------------------------
# Initialize cookies to prevent 403 from NSE
# -----------------------------------------------------
def init_nse_cookies():
    try:
        session.get("https://www.nseindia.com", timeout=10)
        time.sleep(1)
    except:
        pass


# -----------------------------------------------------
# Safe JSON fetch with retry + cookie refresh
# -----------------------------------------------------
def get_json(url):
    for _ in range(5):
        try:
            r = session.get(url, timeout=10)
            return r.json()
        except:
            init_nse_cookies()
            time.sleep(1)
    return None


# -----------------------------------------------------
# 1. Fetch Pre-Market F&O Data
# -----------------------------------------------------
def fetch_premarket_fno():
    url = "https://www.nseindia.com/api/market-data-pre-open?key=FO"
    data = get_json(url)

    if not data:
        print("Failed to fetch Pre-Market FO data")
        return {}

    fno = {}

    for item in data.get("data", []):
        metadata = item.get("metadata", {})

        symbol = metadata.get("symbol")
        pchange = metadata.get("pChange", 0)
        qty = metadata.get("finalQuantity", 0)

        # Only keep stocks ±2% movement
        if symbol and abs(float(pchange)) >= 2:
            fno[symbol] = {
                "symbol": symbol,
                "pchange": float(pchange),
                "qty": int(qty)
            }

    return fno


# -----------------------------------------------------
# 2. Fetch OI Spurts (avgInOI = % change in OI)
# -----------------------------------------------------
def fetch_oi_spurts():
    url = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"
    data = get_json(url)

    if not data:
        print("Failed to fetch OI spurts underlyings")
        return {}

    oi = {}

    for item in data.get("data", []):
        symbol = item.get("symbol")
        avg_oi = float(item.get("avgInOI", 0))  # THIS IS % CHANGE IN OI

        # Filter: OI % change >= 20%
        if avg_oi >= 20:
            oi[symbol] = {
                "symbol": symbol,
                "oi_change_percent": avg_oi,
                "latest_oi": item.get("latestOI"),
                "prev_oi": item.get("prevOI"),
                "change_oi": item.get("changeInOI"),
                "volume": item.get("volume"),
                "underlying": item.get("underlyingValue")
            }

    return oi


# -----------------------------------------------------
# 3. Merge Pre-Market + OI Spurts
# -----------------------------------------------------
def merge_data(pre, oi):
    merged = []

    for symbol in pre:
        if symbol in oi:
            merged.append({
                "symbol": symbol,

                # Pre-Market data
                "pchange": pre[symbol]["pchange"],
                "qty": pre[symbol]["qty"],

                # OI Spurt data
                "oi_change_percent": oi[symbol]["oi_change_percent"],
                "latest_oi": oi[symbol]["latest_oi"],
                "prev_oi": oi[symbol]["prev_oi"],
                "change_oi": oi[symbol]["change_oi"],
                "volume": oi[symbol]["volume"],
                "underlying": oi[symbol]["underlying"],

                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

    return merged


# -----------------------------------------------------
# 4. Save results to daily CSV
# -----------------------------------------------------
def save_to_csv(rows):
    if not rows:
        print("No combined signals found")
        return

    filename = f"{datetime.now().strftime('%Y-%m-%d')}.csv"

    with open(filename, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "Symbol",
            "%Change PreMarket",
            "Qty Traded",
            "%Change OI",
            "Latest OI",
            "Previous OI",
            "Change In OI",
            "Volume",
            "Underlying Price",
            "Timestamp"
        ])

        for r in rows:
            writer.writerow([
                r["symbol"],
                r["pchange"],
                r["qty"],
                r["oi_change_percent"],
                r["latest_oi"],
                r["prev_oi"],
                r["change_oi"],
                r["volume"],
                r["underlying"],
                r["timestamp"]
            ])

    print(f"CSV saved successfully → {filename}")


# -----------------------------------------------------
# MAIN
# -----------------------------------------------------
if __name__ == "__main__":
    init_nse_cookies()

    pre = fetch_premarket_fno()
    oi = fetch_oi_spurts()
    merged = merge_data(pre, oi)

    save_to_csv(merged)
