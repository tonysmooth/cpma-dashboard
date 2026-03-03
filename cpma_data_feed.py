#!/usr/bin/env python3
"""
CPMA Public Comps Data Feed
===========================
Fetches real-time market data for Engineering & Construction public comparables.
Uses Financial Modeling Prep (FMP) API for market data and consensus estimates.

Usage:
    python cpma_data_feed.py                    # Uses demo API key
    python cpma_data_feed.py --api-key YOUR_KEY # Uses your FMP API key
    python cpma_data_feed.py --output data.json # Custom output path

Output: cpma_comps_data.json (consumed by the HTML dashboard)

To get a free FMP API key: https://financialmodelingprep.com/developer/docs/
"""

import json
import time
import argparse
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path

# ─── Company Universe ───────────────────────────────────────────────────────

COMPANIES = [
    # Construction Contractors
    {"ticker": "ACS.MC",    "name": "ACS, Actividades de Construccion y Servicios", "category": "Construction Contractors", "display_ticker": "ACS"},
    {"ticker": "SKA-B.ST",  "name": "Skanska",                                     "category": "Construction Contractors", "display_ticker": "SKA.B"},
    {"ticker": "AGX",       "name": "Argan",                                        "category": "Construction Contractors", "display_ticker": "AGX"},
    {"ticker": "TPC",       "name": "Tutor Perini",                                 "category": "Construction Contractors", "display_ticker": "TPC"},
    {"ticker": "ARE.TO",    "name": "Aecon Group",                                  "category": "Construction Contractors", "display_ticker": "ARE"},
    {"ticker": "BWMN",      "name": "Bowman Consulting",                            "category": "Construction Contractors", "display_ticker": "BWMN"},

    # Diversified Engineering and Construction
    {"ticker": "DG.PA",     "name": "VINCI",                                        "category": "Diversified Engineering and Construction", "display_ticker": "DG"},
    {"ticker": "HOT.DE",    "name": "HOCHTIEF",                                     "category": "Diversified Engineering and Construction", "display_ticker": "HOT"},
    {"ticker": "EN.PA",     "name": "Bouygues",                                     "category": "Diversified Engineering and Construction", "display_ticker": "EN"},
    {"ticker": "J",         "name": "Jacobs Solutions",                              "category": "Diversified Engineering and Construction", "display_ticker": "J"},
    {"ticker": "1802.T",    "name": "Obayashi",                                     "category": "Diversified Engineering and Construction", "display_ticker": "1802"},
    {"ticker": "ACM",       "name": "AECOM",                                        "category": "Diversified Engineering and Construction", "display_ticker": "ACM"},
    {"ticker": "STN.TO",    "name": "Stantec",                                      "category": "Diversified Engineering and Construction", "display_ticker": "STN"},
    {"ticker": "ATRL.TO",   "name": "Atkinsrealis",                                 "category": "Diversified Engineering and Construction", "display_ticker": "ATRL"},
    {"ticker": "KBR",       "name": "KBR",                                          "category": "Diversified Engineering and Construction", "display_ticker": "KBR"},
    {"ticker": "ARCAD.AS",  "name": "Arcadis",                                      "category": "Diversified Engineering and Construction", "display_ticker": "ARCAD"},
    {"ticker": "WBD.MI",    "name": "Webuild",                                      "category": "Diversified Engineering and Construction", "display_ticker": "WBD"},
    {"ticker": "FLR",       "name": "Fluor",                                        "category": "Diversified Engineering and Construction", "display_ticker": "FLR"},

    # Specialty Engineering and Construction
    {"ticker": "FIX",       "name": "Comfort Systems USA",                          "category": "Specialty Engineering and Construction", "display_ticker": "FIX"},
    {"ticker": "EME",       "name": "EMCOR Group",                                  "category": "Specialty Engineering and Construction", "display_ticker": "EME"},
    {"ticker": "WSP.TO",    "name": "WSP Global",                                   "category": "Specialty Engineering and Construction", "display_ticker": "WSP"},
    {"ticker": "APG",       "name": "APi Group Corp",                               "category": "Specialty Engineering and Construction", "display_ticker": "APG"},
    {"ticker": "DY",        "name": "Dycom Industries",                             "category": "Specialty Engineering and Construction", "display_ticker": "DY"},
    {"ticker": "TTEK",      "name": "Tetra Tech",                                   "category": "Specialty Engineering and Construction", "display_ticker": "TTEK"},
    {"ticker": "GBF.DE",    "name": "Bilfinger",                                    "category": "Specialty Engineering and Construction", "display_ticker": "GBF"},

    # Infrastructure Services
    {"ticker": "STRL",      "name": "Sterling Infrastructure",                      "category": "Infrastructure Services", "display_ticker": "STRL"},
    {"ticker": "ROAD",      "name": "Construction Partners",                        "category": "Infrastructure Services", "display_ticker": "ROAD"},
    {"ticker": "GVA",       "name": "Granite Construction",                         "category": "Infrastructure Services", "display_ticker": "GVA"},
    {"ticker": "BBY.L",     "name": "Balfour Beatty",                               "category": "Infrastructure Services", "display_ticker": "BBY"},

    # Utility Services
    {"ticker": "PWR",       "name": "Quanta Services",                              "category": "Utility Services", "display_ticker": "PWR"},
    {"ticker": "MTZ",       "name": "MasTec",                                       "category": "Utility Services", "display_ticker": "MTZ"},
    {"ticker": "PRIM",      "name": "Primoris Services",                            "category": "Utility Services", "display_ticker": "PRIM"},
    {"ticker": "MYRG",      "name": "MYR Group",                                    "category": "Utility Services", "display_ticker": "MYRG"},
    {"ticker": "AMRC",      "name": "Ameresco",                                     "category": "Utility Services", "display_ticker": "AMRC"},

    # Management Consulting
    {"ticker": "IBM",       "name": "IBM",                                          "category": "Management Consulting", "display_ticker": "IBM"},
    {"ticker": "ACN",       "name": "Accenture",                                    "category": "Management Consulting", "display_ticker": "ACN"},
    {"ticker": "BAH",       "name": "Booz Allen Hamilton",                          "category": "Management Consulting", "display_ticker": "BAH"},
    {"ticker": "FCN",       "name": "FTI Consulting",                               "category": "Management Consulting", "display_ticker": "FCN"},
    {"ticker": "HURN",      "name": "Huron Consulting",                             "category": "Management Consulting", "display_ticker": "HURN"},
]

FMP_BASE = "https://financialmodelingprep.com/api/v3"

# ─── API Helpers ────────────────────────────────────────────────────────────

def fmp_get(endpoint, api_key, params=None):
    """Make a GET request to FMP API."""
    url = f"{FMP_BASE}/{endpoint}?apikey={api_key}"
    if params:
        for k, v in params.items():
            url += f"&{k}={v}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CPMA-Comps/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, json.JSONDecodeError) as e:
        print(f"  [WARN] API error for {endpoint}: {e}")
        return None


def fetch_quote(ticker, api_key):
    """Fetch current quote data for a ticker."""
    data = fmp_get(f"quote/{ticker}", api_key)
    if data and len(data) > 0:
        return data[0]
    return None


def fetch_key_metrics(ticker, api_key, period="annual", limit=3):
    """Fetch key metrics (includes EV, margins, etc.)."""
    data = fmp_get(f"key-metrics/{ticker}", api_key, {"period": period, "limit": limit})
    return data if data else []


def fetch_income_statement(ticker, api_key, period="annual", limit=3):
    """Fetch income statement data."""
    data = fmp_get(f"income-statement/{ticker}", api_key, {"period": period, "limit": limit})
    return data if data else []


def fetch_analyst_estimates(ticker, api_key, limit=5):
    """Fetch analyst consensus estimates."""
    data = fmp_get(f"analyst-estimates/{ticker}", api_key, {"limit": limit})
    return data if data else []


def fetch_enterprise_value(ticker, api_key, limit=3):
    """Fetch enterprise value data."""
    data = fmp_get(f"enterprise-values/{ticker}", api_key, {"period": "annual", "limit": limit})
    return data if data else []


def fetch_historical_price(ticker, api_key, from_date, to_date):
    """Fetch historical daily prices for stock price performance chart."""
    data = fmp_get(f"historical-price-full/{ticker}", api_key,
                   {"from": from_date, "to": to_date})
    if data and "historical" in data:
        return data["historical"]
    return []


# ─── Data Processing ────────────────────────────────────────────────────────

def safe_div(a, b):
    """Safe division returning None if not possible."""
    if a is None or b is None or b == 0:
        return None
    return a / b


def process_company(company, api_key):
    """Fetch and process all data for a single company."""
    ticker = company["ticker"]
    print(f"  Fetching {ticker} ({company['name']})...")

    result = {
        "ticker": company["display_ticker"],
        "fmp_ticker": ticker,
        "name": company["name"],
        "category": company["category"],
    }

    # 1. Current quote
    quote = fetch_quote(ticker, api_key)
    if quote:
        result["share_price"] = quote.get("price")
        result["market_cap"] = safe_div(quote.get("marketCap"), 1_000_000)  # in $M
        result["year_high"] = quote.get("yearHigh")
        result["year_low"] = quote.get("yearLow")
        result["pct_52wk"] = safe_div(quote.get("price"), quote.get("yearHigh")) * 100 if quote.get("yearHigh") else None
        result["eps"] = quote.get("eps")
        result["pe"] = quote.get("pe")

    time.sleep(0.3)  # Rate limiting

    # 2. Enterprise value
    ev_data = fetch_enterprise_value(ticker, api_key, limit=1)
    if ev_data:
        result["enterprise_value"] = safe_div(ev_data[0].get("enterpriseValue"), 1_000_000)
    elif quote:
        # Fallback: approximate EV from market cap
        result["enterprise_value"] = result.get("market_cap")

    time.sleep(0.3)

    # 3. Income statements (actuals)
    income = fetch_income_statement(ticker, api_key, limit=3)
    if income:
        # Most recent year
        latest = income[0]
        prev = income[1] if len(income) > 1 else None

        latest_rev = latest.get("revenue", 0)
        latest_ebitda = latest.get("ebitda", 0)
        latest_ni = latest.get("netIncome", 0)

        result["revenue_2024"] = safe_div(latest_rev, 1_000_000)
        result["ebitda_2024"] = safe_div(latest_ebitda, 1_000_000)
        result["ebitda_margin_2024"] = safe_div(latest_ebitda, latest_rev)

        if prev:
            prev_rev = prev.get("revenue", 0)
            result["rev_growth_2024"] = safe_div(latest_rev - prev_rev, prev_rev) if prev_rev else None

    time.sleep(0.3)

    # 4. Analyst estimates (forward)
    estimates = fetch_analyst_estimates(ticker, api_key, limit=5)
    est_by_year = {}
    for est in estimates:
        year = est.get("date", "")[:4]
        if year:
            est_by_year[year] = est

    current_year = datetime.now().year
    fy1 = str(current_year)       # 2025E
    fy2 = str(current_year + 1)   # 2026E

    if fy1 in est_by_year:
        e = est_by_year[fy1]
        result["revenue_2025e"] = safe_div(e.get("estimatedRevenueAvg"), 1_000_000)
        result["ebitda_2025e"] = safe_div(e.get("estimatedEbitdaAvg"), 1_000_000)
        result["net_income_2025e"] = safe_div(e.get("estimatedNetIncomeAvg"), 1_000_000)
        if result.get("revenue_2025e") and result.get("ebitda_2025e"):
            result["ebitda_margin_2025"] = safe_div(e.get("estimatedEbitdaAvg"), e.get("estimatedRevenueAvg"))
        if result.get("revenue_2024") and result.get("revenue_2025e"):
            result["rev_growth_2025"] = safe_div(
                result["revenue_2025e"] - result["revenue_2024"], result["revenue_2024"]
            )

    if fy2 in est_by_year:
        e = est_by_year[fy2]
        result["revenue_2026e"] = safe_div(e.get("estimatedRevenueAvg"), 1_000_000)
        result["ebitda_2026e"] = safe_div(e.get("estimatedEbitdaAvg"), 1_000_000)
        result["net_income_2026e"] = safe_div(e.get("estimatedNetIncomeAvg"), 1_000_000)
        if result.get("revenue_2026e") and result.get("ebitda_2026e"):
            result["ebitda_margin_2026"] = safe_div(e.get("estimatedEbitdaAvg"), e.get("estimatedRevenueAvg"))

    # 5. Compute trading multiples
    ev = result.get("enterprise_value")
    if ev:
        for suffix, rev_key, ebitda_key in [
            ("_2024", "revenue_2024", "ebitda_2024"),
            ("_2025", "revenue_2025e", "ebitda_2025e"),
            ("_2026", "revenue_2026e", "ebitda_2026e"),
        ]:
            rev = result.get(rev_key)
            ebitda = result.get(ebitda_key)
            result[f"ev_rev{suffix}"] = safe_div(ev, rev)
            result[f"ev_ebitda{suffix}"] = safe_div(ev, ebitda)

    # P/E multiples
    mc = result.get("market_cap")
    if mc:
        for suffix, ni_key in [
            ("_2025", "net_income_2025e"),
            ("_2026", "net_income_2026e"),
        ]:
            ni = result.get(ni_key)
            result[f"pe{suffix}"] = safe_div(mc, ni)
        # 2024A P/E from income statement
        if income:
            ni_2024 = safe_div(income[0].get("netIncome", 0), 1_000_000)
            result["pe_2024"] = safe_div(mc, ni_2024) if ni_2024 and ni_2024 > 0 else None

    return result


def fetch_price_history(companies, api_key):
    """Fetch 1-year price history for all companies for the stock performance chart."""
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    price_data = {}
    for company in companies:
        ticker = company["ticker"]
        print(f"  Fetching price history for {ticker}...")
        prices = fetch_historical_price(ticker, api_key, from_date, to_date)
        if prices:
            # Sort by date ascending
            prices.sort(key=lambda x: x["date"])
            # Index to 100
            base_price = prices[0]["close"]
            indexed = [
                {"date": p["date"], "indexed": round(p["close"] / base_price * 100, 2)}
                for p in prices
            ]
            price_data[company["display_ticker"]] = indexed
        time.sleep(0.3)

    return price_data


def compute_category_averages(companies_data, categories):
    """Compute mean and median for each category."""
    import statistics

    metrics = [
        "rev_growth_2024", "rev_growth_2025",
        "ebitda_margin_2024", "ebitda_margin_2025", "ebitda_margin_2026",
        "ev_rev_2024", "ev_rev_2025", "ev_rev_2026",
        "ev_ebitda_2024", "ev_ebitda_2025", "ev_ebitda_2026",
        "pe_2024", "pe_2025", "pe_2026",
    ]

    summaries = {}
    for cat in categories:
        cat_companies = [c for c in companies_data if c["category"] == cat]
        cat_summary = {"mean": {}, "median": {}}

        for m in metrics:
            values = [c[m] for c in cat_companies if c.get(m) is not None and c[m] > 0]
            if values:
                cat_summary["mean"][m] = round(statistics.mean(values), 4)
                cat_summary["median"][m] = round(statistics.median(values), 4)

        summaries[cat] = cat_summary

    # Overall
    all_vals = {}
    for m in metrics:
        values = [c[m] for c in companies_data if c.get(m) is not None and c[m] > 0]
        if values:
            all_vals[m] = {
                "mean": round(statistics.mean(values), 4),
                "median": round(statistics.median(values), 4),
            }
    summaries["Overall"] = all_vals

    return summaries


# ─── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CPMA Public Comps Data Feed")
    parser.add_argument("--api-key", default="demo", help="FMP API key (default: demo)")
    parser.add_argument("--output", default=None, help="Output JSON path")
    parser.add_argument("--skip-prices", action="store_true", help="Skip historical price fetch")
    args = parser.parse_args()

    output_path = args.output or str(Path(__file__).parent / "cpma_comps_data.json")

    print("=" * 60)
    print("CPMA Public Comps Data Feed")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"API Key: {'custom' if args.api_key != 'demo' else 'demo (limited)'}")
    print("=" * 60)

    # Fetch company data
    print("\n[1/3] Fetching company fundamentals and estimates...")
    companies_data = []
    for company in COMPANIES:
        try:
            data = process_company(company, args.api_key)
            companies_data.append(data)
        except Exception as e:
            print(f"  [ERROR] Failed for {company['ticker']}: {e}")
            companies_data.append({
                "ticker": company["display_ticker"],
                "fmp_ticker": company["ticker"],
                "name": company["name"],
                "category": company["category"],
                "error": str(e),
            })

    # Fetch price history
    price_history = {}
    if not args.skip_prices:
        print("\n[2/3] Fetching 1-year price history...")
        price_history = fetch_price_history(COMPANIES, args.api_key)

    # Compute category summaries
    print("\n[3/3] Computing category summaries...")
    categories = [
        "Construction Contractors",
        "Diversified Engineering and Construction",
        "Specialty Engineering and Construction",
        "Infrastructure Services",
        "Utility Services",
        "Management Consulting",
    ]
    summaries = compute_category_averages(companies_data, categories)

    # Build output
    output = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "source": "Financial Modeling Prep API",
            "num_companies": len(companies_data),
            "categories": categories,
        },
        "companies": companies_data,
        "category_summaries": summaries,
        "price_history": price_history,
    }

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n{'=' * 60}")
    print(f"Data saved to: {output_path}")
    print(f"Companies processed: {len(companies_data)}")
    print(f"Price series: {len(price_history)} tickers")

    # Summary of data quality
    has_price = sum(1 for c in companies_data if c.get("share_price"))
    has_ev = sum(1 for c in companies_data if c.get("enterprise_value"))
    has_est = sum(1 for c in companies_data if c.get("revenue_2025e"))
    print(f"  With price data: {has_price}/{len(companies_data)}")
    print(f"  With EV data: {has_ev}/{len(companies_data)}")
    print(f"  With estimates: {has_est}/{len(companies_data)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
