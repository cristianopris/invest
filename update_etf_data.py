#!/usr/bin/env python3
"""
update_etf_data.py  —  Fetch fresh data and patch ETFComparison.html in-place.

Usage:
    pip install yfinance requests beautifulsoup4 lxml
    python update_etf_data.py

Sources:
    Holdings   → justetf.com (scraped, top 15 per ETF)
    ETF rets   → justetf.com (scraped) with yfinance fallback
    Stock rets → Yahoo Finance via yfinance
"""

import re, sys, time
from datetime import date, datetime, timedelta
from pathlib import Path

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    sys.exit("Missing dep — run: pip install yfinance pandas")

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("Missing dep — run: pip install requests beautifulsoup4 lxml")

# ── Config ────────────────────────────────────────────────────────────────────

HTML_PATH = Path(__file__).parent / "ETFComparison.html"
TODAY     = date.today()
TODAY_STR = TODAY.strftime("%b %d, %Y")

ETF_ISINS = {
    "XUTC": "IE00BGQYRS42",
    "SP20": "IE000VA628D5",
    "EQQQ": "IE0032077012",
    "WTAI": "IE00BDVPNG13",
    "CSPX": "IE00B5BMR087",
    "VWCE": "IE00BK5BQT80",
}

# LSE tickers used as yfinance fallback for ETF returns
ETF_YF_TICKERS = {
    "XUTC": "XUTC.L",
    "SP20": "SP20.L",
    "EQQQ": "EQQQ.L",
    "WTAI": "WTAI.L",
    "CSPX": "CSPX.L",
    "VWCE": "VWCE.L",
}

# Calendar days per period label
PERIOD_DAYS = {
    "1M": 31,
    "3M": 92,
    "6M": 183,
    "1Y": 365,
    "3Y": 365 * 3,
    "5Y": 365 * 5,
}

# Tickers that appear in HOLDING_RETURNS (BRK_B → BRK-B for yfinance)
STOCK_TICKERS = [
    "NVDA", "AAPL", "MSFT", "AVGO", "MU", "AMD", "PLTR", "CSCO",
    "LRCX", "IBM", "ACN", "AMAT", "TXN", "CRM", "INTU", "META",
    "GOOGL", "GOOG", "VZ", "CMCSA", "NFLX", "T", "EA", "TTWO",
    "DIS", "WBD", "LYV", "CHTR", "OMC", "NWSA", "AMZN", "TSLA",
    "COST", "PEP", "TSM", "JPM", "LLY", "ORCL", "SAP", "BRK-B",
    "V", "XOM", "UNH", "WMT", "HD", "QCOM", "000660.KS",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def pct_change(series: "pd.Series", days: int) -> "float | None":
    """% change over the last `days` calendar days in a price series."""
    if series.empty:
        return None
    end_price   = float(series.iloc[-1])
    cutoff_date = series.index[-1] - timedelta(days=days)
    window      = series[series.index >= cutoff_date]
    if window.empty:
        return None
    start_price = float(window.iloc[0])
    if start_price == 0:
        return None
    return round((end_price / start_price - 1) * 100, 1)


def justetf_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        s.get("https://www.justetf.com/en/", timeout=10)
    except Exception:
        pass
    return s


# ── Holdings scraper ──────────────────────────────────────────────────────────

# JustETF renders holdings via JS/AJAX, so we hit their internal JSON endpoint.
# The endpoint is: /servlet/etf-and-index-data?isin=<ISIN>&locale=en
# which returns a JSON payload containing "topHoldings".

def fetch_holdings_api(session: requests.Session, etf: str, isin: str, top_n: int = 15):
    """Fetch top holdings via JustETF's internal JSON servlet."""
    url = (
        f"https://www.justetf.com/servlet/etf-and-index-data"
        f"?isin={isin}&locale=en&currency=USD"
    )
    print(f"  {etf}: fetching holdings from API …")
    try:
        r = session.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"    ERROR: {e}")
        return None

    raw = data.get("topHoldings") or data.get("holdings") or []
    if not raw:
        # Try nested structure
        for key in data:
            if isinstance(data[key], list) and data[key] and "weight" in str(data[key][0]).lower():
                raw = data[key]
                break

    if not raw:
        print(f"    WARNING: no holdings found in API response for {etf}")
        return None

    holdings = []
    for item in raw[:top_n]:
        ticker = (
            item.get("ticker") or item.get("symbol") or item.get("isin") or ""
        ).strip()
        name   = (item.get("name") or item.get("description") or "").strip()
        weight = float(item.get("weight") or item.get("percentage") or 0)
        if weight > 0:
            holdings.append({"ticker": ticker, "name": name, "weight": round(weight, 2)})

    if holdings:
        print(f"    Got {len(holdings)} holdings")
        return holdings

    # Fall back to HTML scraping
    return fetch_holdings_html(session, etf, isin, top_n)


def fetch_holdings_html(session: requests.Session, etf: str, isin: str, top_n: int = 15):
    """Scrape top holdings from the JustETF profile HTML page."""
    url = f"https://www.justetf.com/en/etf-profile.html?isin={isin}#holdings"
    print(f"  {etf}: scraping holdings from HTML …")
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"    ERROR: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")
    holdings = []

    # JustETF holdings table selectors (they change occasionally)
    for selector in [
        "table.etf-holdings tbody tr",
        "div#etf-holdings table tbody tr",
        "div.holdings-table tbody tr",
        "section.etf-holdings-section table tbody tr",
        "table tbody tr",          # last-resort: first table on page
    ]:
        rows = soup.select(selector)
        if not rows:
            continue
        for row in rows[:top_n]:
            cells = [td.get_text(" ", strip=True) for td in row.select("td")]
            if len(cells) < 3:
                continue
            # Weight is usually the last numeric cell
            weight_raw = cells[-1].replace(",", ".").replace("%", "").strip()
            try:
                weight = round(float(weight_raw), 2)
            except ValueError:
                continue
            if weight <= 0:
                continue
            # Name / ticker heuristic: take first two non-numeric cells
            name   = cells[1] if len(cells) > 1 else cells[0]
            ticker = cells[2] if len(cells) > 2 else ""
            holdings.append({"ticker": ticker, "name": name, "weight": weight})
        if holdings:
            break

    if holdings:
        print(f"    Got {len(holdings)} holdings")
        return holdings

    print(f"    WARNING: could not parse holdings for {etf}")
    return None


# ── ETF returns scraper ───────────────────────────────────────────────────────

def fetch_etf_returns_api(session: requests.Session, etf: str, isin: str):
    """Fetch performance data via JustETF's internal JSON endpoint."""
    url = (
        f"https://www.justetf.com/servlet/etf-and-index-data"
        f"?isin={isin}&locale=en&currency=USD"
    )
    print(f"  {etf}: fetching returns from API …")
    try:
        r = session.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"    ERROR: {e}")
        return {}

    LABEL_MAP = {
        "1m": "1M", "1M": "1M",
        "3m": "3M", "3M": "3M",
        "6m": "6M", "6M": "6M",
        "1y": "1Y", "1Y": "1Y",
        "3y": "3Y", "3Y": "3Y",
        "5y": "5Y", "5Y": "5Y",
    }

    returns = {}
    # Try common key patterns
    for raw_key in ["performance", "returns", "historicPerformance", "totalReturn"]:
        perf = data.get(raw_key)
        if isinstance(perf, dict):
            for k, v in perf.items():
                mapped = LABEL_MAP.get(k)
                if mapped and v is not None:
                    try:
                        returns[mapped] = round(float(v) * 100, 1)   # might be decimal
                    except (TypeError, ValueError):
                        pass
            if returns:
                break
        elif isinstance(perf, list):
            for item in perf:
                period = LABEL_MAP.get(item.get("period") or item.get("label") or "")
                if period:
                    try:
                        returns[period] = round(float(item.get("value") or item.get("return") or 0) * 100, 1)
                    except (TypeError, ValueError):
                        pass
            if returns:
                break

    if returns:
        print(f"    Returns: {returns}")
        return returns

    # Fallback: scrape the HTML performance table
    return fetch_etf_returns_html(session, etf, isin)


def fetch_etf_returns_html(session: requests.Session, etf: str, isin: str):
    """Scrape ETF performance from JustETF profile HTML."""
    url = f"https://www.justetf.com/en/etf-profile.html?isin={isin}#returns"
    print(f"  {etf}: scraping returns from HTML …")
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"    ERROR: {e}")
        return {}

    soup = BeautifulSoup(r.text, "lxml")
    LABEL_MAP = {
        "1 month": "1M", "1m": "1M",
        "3 months": "3M", "3m": "3M",
        "6 months": "6M", "6m": "6M",
        "1 year": "1Y", "1y": "1Y",
        "3 years": "3Y", "3y": "3Y",
        "5 years": "5Y", "5y": "5Y",
    }

    returns = {}
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        for row in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if not cells:
                continue
            for i, h in enumerate(headers):
                mapped = LABEL_MAP.get(h)
                if mapped and i < len(cells):
                    raw = cells[i].replace("%", "").replace(",", ".").strip()
                    try:
                        returns[mapped] = round(float(raw), 1)
                    except ValueError:
                        pass

    if returns:
        print(f"    Returns: {returns}")
        return returns

    # Final fallback: yfinance
    return fetch_etf_returns_yf(etf)


def fetch_etf_returns_yf(etf: str):
    """Fallback: compute ETF returns from yfinance price history."""
    yf_ticker = ETF_YF_TICKERS.get(etf)
    if not yf_ticker:
        return {}
    print(f"  {etf}: yfinance fallback ({yf_ticker}) …")
    try:
        hist = yf.Ticker(yf_ticker).history(period="5y", auto_adjust=True)
        if hist.empty:
            return {}
        returns = {}
        for label, days in PERIOD_DAYS.items():
            v = pct_change(hist["Close"], days)
            if v is not None:
                returns[label] = v
        print(f"    Returns: {returns}")
        return returns
    except Exception as e:
        print(f"    yfinance failed: {e}")
        return {}


# ── Stock returns (yfinance) ──────────────────────────────────────────────────

def fetch_stock_returns(tickers: list) -> dict:
    """
    Download 5-year price history for all tickers in one batch,
    then compute % change for each period. Returns USD values.
    """
    print(f"Fetching stock returns for {len(tickers)} tickers …")

    # yfinance uses BRK-B not BRK_B
    yf_tickers = [t.replace("BRK_B", "BRK-B") for t in tickers]

    try:
        raw = yf.download(
            yf_tickers,
            period="5y",
            auto_adjust=True,
            progress=True,
            group_by="ticker",
            threads=True,
        )
    except Exception as e:
        print(f"  Batch download failed ({e}), falling back to individual …")
        raw = None

    results = {}
    for ticker, yf_ticker in zip(tickers, yf_tickers):
        try:
            if raw is not None and isinstance(raw.columns, pd.MultiIndex):
                series = raw["Close"][yf_ticker].dropna()
            elif raw is not None and len(yf_tickers) == 1:
                series = raw["Close"].dropna()
            else:
                # Individual download fallback
                series = yf.Ticker(yf_ticker).history(period="5y", auto_adjust=True)["Close"].dropna()

            if series.empty:
                raise ValueError("empty series")

            rets = {}
            for label, days in PERIOD_DAYS.items():
                rets[label] = pct_change(series, days)
            results[ticker] = rets

        except Exception as e:
            print(f"  {ticker}: ERROR — {e}")
            results[ticker] = {p: None for p in PERIOD_DAYS}

        # Progress
        r = results[ticker]
        line = "  ".join(
            f"{p}:{r[p]:+.1f}%" if r.get(p) is not None else f"{p}:N/A"
            for p in PERIOD_DAYS
        )
        print(f"  {ticker:<15s}  {line}")

    return results


# ── JS formatters ─────────────────────────────────────────────────────────────

def js_holdings(holdings_by_etf: dict) -> str:
    lines = ["const RAW_HOLDINGS = {"]
    for etf in ["XUTC", "SP20", "EQQQ", "WTAI", "CSPX", "VWCE"]:
        holdings = holdings_by_etf.get(etf, [])
        lines.append(f"  {etf}: [")
        for h in holdings:
            t = h["ticker"].replace('"', '\\"')
            n = h["name"].replace('"', '\\"')
            w = h["weight"]
            lines.append(f'    {{ ticker:"{t}", name:"{n}", weight:{w:5.2f} }},')
        lines.append("  ],")
    lines.append("};")
    return "\n".join(lines)


def js_etf_returns(returns_by_etf: dict) -> str:
    lines = ["const ETF_RETURNS = {"]
    for etf in ["XUTC", "SP20", "EQQQ", "WTAI", "CSPX", "VWCE"]:
        rets = returns_by_etf.get(etf, {})
        parts = []
        for p in ["1M", "3M", "6M", "1Y", "3Y", "5Y"]:
            v = rets.get(p)
            parts.append(f'"{p}":  null ' if v is None else f'"{p}": {v:5.1f}')
        lines.append(f'  {etf}: {{ {", ".join(parts)} }},')
    lines.append("};")
    return "\n".join(lines)


def js_holding_returns(returns_by_ticker: dict) -> str:
    lines = ["const HOLDING_RETURNS = {"]
    for ticker, rets in returns_by_ticker.items():
        # JS key: quote if it contains dots or isn't a plain identifier
        key = f'"{ticker}"' if ("." in ticker or not ticker.replace("_", "").isalnum()) else ticker
        parts = []
        for p in ["1M", "3M", "6M", "1Y", "3Y", "5Y"]:
            v = rets.get(p)
            parts.append(f'"{p}":   null' if v is None else f'"{p}": {v:7.1f}')
        lines.append(f'  {key}:  {{ {", ".join(parts)} }},')
    lines.append("};")
    return "\n".join(lines)


# ── HTML patcher ──────────────────────────────────────────────────────────────

def patch_html(
    html: str,
    new_holdings: dict | None,
    new_etf_rets: dict | None,
    new_stock_rets: dict | None,
    date_str: str,
) -> str:
    changed = []

    if new_holdings:
        html = re.sub(
            r"const RAW_HOLDINGS = \{.*?\};",
            js_holdings(new_holdings),
            html, flags=re.DOTALL,
        )
        changed.append("RAW_HOLDINGS")

    if new_etf_rets:
        html = re.sub(
            r"const ETF_RETURNS = \{.*?\};",
            js_etf_returns(new_etf_rets),
            html, flags=re.DOTALL,
        )
        changed.append("ETF_RETURNS")

    if new_stock_rets:
        html = re.sub(
            r"const HOLDING_RETURNS = \{.*?\};",
            js_holding_returns(new_stock_rets),
            html, flags=re.DOTALL,
        )
        changed.append("HOLDING_RETURNS")

    # Update dates in header comment and footer
    html = re.sub(
        r"EMBEDDED DATA — compiled \w+ \d+, \d{4}",
        f"EMBEDDED DATA — compiled {date_str}",
        html,
    )
    html = re.sub(
        r"Data as of \w+ \d+, \d{4}\.",
        f"Data as of {date_str}.",
        html,
    )

    for block in changed:
        print(f"  ✓ {block} replaced")

    return html


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"  ETF Data Updater  —  {TODAY_STR}")
    print(f"{'='*60}\n")

    if not HTML_PATH.exists():
        sys.exit(f"ERROR: {HTML_PATH} not found")

    html = HTML_PATH.read_text(encoding="utf-8")

    # 1. Stock returns via yfinance ─────────────────────────────────────────
    print("▶ Step 1 — Individual stock returns (Yahoo Finance / yfinance)")
    print("-" * 60)
    stock_rets = fetch_stock_returns(STOCK_TICKERS)
    # Rename BRK-B → BRK_B for JS key compatibility
    if "BRK-B" in stock_rets:
        stock_rets["BRK_B"] = stock_rets.pop("BRK-B")
    print()

    # 2. ETF data from JustETF ──────────────────────────────────────────────
    print("▶ Step 2 — ETF holdings & returns (JustETF)")
    print("-" * 60)
    session = justetf_session()
    holdings_by_etf = {}
    etf_rets_by_etf = {}

    for etf, isin in ETF_ISINS.items():
        h = fetch_holdings_api(session, etf, isin)
        if h:
            holdings_by_etf[etf] = h
        time.sleep(1.5)   # polite crawl delay

        r = fetch_etf_returns_api(session, etf, isin)
        if r:
            etf_rets_by_etf[etf] = r
        time.sleep(1.5)
    print()

    # 3. Patch & write ─────────────────────────────────────────────────────
    print("▶ Step 3 — Patching ETFComparison.html")
    print("-" * 60)
    new_html = patch_html(
        html,
        new_holdings  = holdings_by_etf  or None,
        new_etf_rets  = etf_rets_by_etf  or None,
        new_stock_rets= stock_rets        or None,
        date_str      = TODAY_STR,
    )
    HTML_PATH.write_text(new_html, encoding="utf-8")
    print(f"\n✅  {HTML_PATH.name} updated — {TODAY_STR}\n")


if __name__ == "__main__":
    main()
