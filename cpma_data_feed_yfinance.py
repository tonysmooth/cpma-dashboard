#!/usr/bin/env python3
"""
CPMA Public Comps Data Feed (yfinance + Perplexity Sonar API)
=============================================================
Fetches real-time market data for Engineering & Construction public comparables.
Uses yfinance for price/fundamentals and optionally Perplexity for forward estimates.

Usage:
    python cpma_data_feed_yfinance.py                        # Uses PERPLEXITY_API_KEY env var
    python cpma_data_feed_yfinance.py --no-perplexity        # yfinance only
    python cpma_data_feed_yfinance.py --output data.json     # Custom output path

Output: cpma_comps_data.json (consumed by the HTML dashboard)
"""

import json
import os
import sys
import time
import argparse
import statistics
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
#  Company Universe
# ---------------------------------------------------------------------------

COMPANIES = [
    # Construction Contractors
    {"ticker": "ACS.MC",    "name": "ACS, Actividades de Construccion y Servicios", "category": "Construction Contractors",                    "display_ticker": "ACS"},
    {"ticker": "SKA-B.ST",  "name": "Skanska",                                     "category": "Construction Contractors",                    "display_ticker": "SKA.B"},
    {"ticker": "AGX",       "name": "Argan",                                        "category": "Construction Contractors",                    "display_ticker": "AGX"},
    {"ticker": "TPC",       "name": "Tutor Perini",                                 "category": "Construction Contractors",                    "display_ticker": "TPC"},
    {"ticker": "ARE.TO",    "name": "Aecon Group",                                  "category": "Construction Contractors",                    "display_ticker": "ARE"},
    {"ticker": "BWMN",      "name": "Bowman Consulting",                            "category": "Construction Contractors",                    "display_ticker": "BWMN"},

    # Diversified Engineering and Construction
    {"ticker": "DG.PA",     "name": "VINCI",                                        "category": "Diversified Engineering and Construction",    "display_ticker": "DG"},
    {"ticker": "HOT.DE",    "name": "HOCHTIEF",                                     "category": "Diversified Engineering and Construction",    "display_ticker": "HOT"},
    {"ticker": "EN.PA",     "name": "Bouygues",                                     "category": "Diversified Engineering and Construction",    "display_ticker": "EN"},
    {"ticker": "J",         "name": "Jacobs Solutions",                             "category": "Diversified Engineering and Construction",    "display_ticker": "J"},
    {"ticker": "1802.T",    "name": "Obayashi Corporation",                         "category": "Diversified Engineering and Construction",    "display_ticker": "1802"},
    {"ticker": "ACM",       "name": "AECOM",                                        "category": "Diversified Engineering and Construction",    "display_ticker": "ACM"},
    {"ticker": "STN",       "name": "Stantec",                                      "category": "Diversified Engineering and Construction",    "display_ticker": "STN"},
    {"ticker": "ATRL.PA",   "name": "Altarea",                                      "category": "Diversified Engineering and Construction",    "display_ticker": "ATRL"},
    {"ticker": "KBR",       "name": "KBR Inc",                                      "category": "Diversified Engineering and Construction",    "display_ticker": "KBR"},
    {"ticker": "ARCAD.PA",  "name": "Arcadis",                                      "category": "Diversified Engineering and Construction",    "display_ticker": "ARCAD"},
    {"ticker": "WBD",       "name": "Webuild",                                      "category": "Diversified Engineering and Construction",    "display_ticker": "WBD"},
    {"ticker": "FLR",       "name": "Fluor Corporation",                            "category": "Diversified Engineering and Construction",    "display_ticker": "FLR"},

    # Specialty Engineering and Construction
    {"ticker": "FIX",       "name": "Comfort Systems USA",                          "category": "Specialty Engineering and Construction",      "display_ticker": "FIX"},
    {"ticker": "EME",       "name": "EMCOR Group",                                  "category": "Specialty Engineering and Construction",      "display_ticker": "EME"},
    {"ticker": "WSP.TO",    "name": "WSP Global",                                   "category": "Specialty Engineering and Construction",      "display_ticker": "WSP"},
    {"ticker": "APG.AX",    "name": "APM Human Services International",             "category": "Specialty Engineering and Construction",      "display_ticker": "APG"},
    {"ticker": "DY",        "name": "Dycom Industries",                             "category": "Specialty Engineering and Construction",      "display_ticker": "DY"},
    {"ticker": "TTEK",      "name": "Tetra Tech",                                   "category": "Specialty Engineering and Construction",      "display_ticker": "TTEK"},
    {"ticker": "GBF.VI",    "name": "Strabag SE",                                   "category": "Specialty Engineering and Construction",      "display_ticker": "GBF"},

    # Infrastructure Services
    {"ticker": "STRL",      "name": "Sterling Infrastructure",                      "category": "Infrastructure Services",                    "display_ticker": "STRL"},
    {"ticker": "ROAD",      "name": "Construction Partners",                        "category": "Infrastructure Services",                    "display_ticker": "ROAD"},
    {"ticker": "GVA",       "name": "Granite Construction",                         "category": "Infrastructure Services",                    "display_ticker": "GVA"},
    {"ticker": "BBY",       "name": "Balfour Beatty",                               "category": "Infrastructure Services",                    "display_ticker": "BBY"},

    # Utility Services
    {"ticker": "PWR",       "name": "Quanta Services",                              "category": "Utility Services",                           "display_ticker": "PWR"},
    {"ticker": "MTZ",       "name": "MasTec",                                       "category": "Utility Services",                           "display_ticker": "MTZ"},
    {"ticker": "PRIM",      "name": "Primoris Services",                            "category": "Utility Services",                           "display_ticker": "PRIM"},
    {"ticker": "MYRG",      "name": "MYR Group",                                    "category": "Utility Services",                           "display_ticker": "MYRG"},
    {"ticker": "AMRC",      "name": "Ameresco",                                     "category": "Utility Services",                           "display_ticker": "AMRC"},

    # Management Consulting
    {"ticker": "IBM",       "name": "IBM",                                          "category": "Management Consulting",                      "display_ticker": "IBM"},
    {"ticker": "ACN",       "name": "Accenture",                                    "category": "Management Consulting",                      "display_ticker": "ACN"},
    {"ticker": "BAH",       "name": "Booz Allen Hamilton",                          "category": "Management Consulting",                      "display_ticker": "BAH"},
    {"ticker": "FCN",       "name": "FTI Consulting",                               "category": "Management Consulting",                      "display_ticker": "FCN"},
    {"ticker": "HURN",      "name": "Huron Consulting Group",                       "category": "Management Consulting",                      "display_ticker": "HURN"},
]

CATEGORIES = [
    "Construction Contractors",
    "Diversified Engineering and Construction",
    "Specialty Engineering and Construction",
    "Infrastructure Services",
    "Utility Services",
    "Management Consulting",
]

# Some tickers need special handling for yfinance
# BBY on London = BBY.L, WBD on Milan = WBD.MI, ARCAD on Amsterdam = ARCAD.AS
YFINANCE_TICKER_OVERRIDES = {
    "BBY":      "BBY.L",
    "WBD":      "WBD.MI",
    "ARCAD.PA": "ARCAD.AS",
}


# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

def safe_div(a, b):
    """Safe division returning None if divisor is zero or args are None."""
    if a is None or b is None or b == 0:
        return None
    return a / b


def millions(val):
    """Convert a value to millions (yfinance returns raw numbers)."""
    if val is None:
        return None
    try:
        v = float(val)
        return v / 1_000_000 if v != 0 else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
#  yfinance data fetching
# ---------------------------------------------------------------------------

def fetch_yfinance_data(ticker_str):
    """Fetch financial data for a single company using yfinance."""
    import yfinance as yf

    yf_ticker = YFINANCE_TICKER_OVERRIDES.get(ticker_str, ticker_str)
    print(f"    Fetching yfinance data for {ticker_str} (as {yf_ticker})...")

    try:
        tk = yf.Ticker(yf_ticker)
        info = tk.info or {}

        # Basic price/valuation data
        data = {
            "share_price": info.get("currentPrice") or info.get("regularMarketPrice"),
            "market_cap": millions(info.get("marketCap")),
            "enterprise_value": millions(info.get("enterpriseValue")),
            "year_high": info.get("fiftyTwoWeekHigh"),
            "year_low": info.get("fiftyTwoWeekLow"),
            "trailing_pe": info.get("trailingPE"),
            "forward_pe": info.get("forwardPE"),
        }

        # Historical financials from income statement
        try:
            inc = tk.income_stmt
            if inc is not None and not inc.empty:
                # Income statement columns are dates, most recent first
                cols = sorted(inc.columns, reverse=True)

                # Try to get FY2024 and FY2023 data
                for col in cols:
                    year = col.year if hasattr(col, 'year') else None
                    if year == 2024:
                        data["revenue_2024"] = millions(inc.at["Total Revenue", col]) if "Total Revenue" in inc.index else None
                        data["ebitda_2024"] = millions(inc.at["EBITDA", col]) if "EBITDA" in inc.index else None
                        data["net_income_2024"] = millions(inc.at["Net Income", col]) if "Net Income" in inc.index else None
                    elif year == 2023:
                        data["revenue_2023"] = millions(inc.at["Total Revenue", col]) if "Total Revenue" in inc.index else None
        except Exception as e:
            print(f"    [WARN] Income statement error for {ticker_str}: {e}")

        # Forward estimates from analyst data
        try:
            rev_est = tk.revenue_estimate
            if rev_est is not None and not rev_est.empty:
                for col in rev_est.columns:
                    col_str = str(col)
                    if "2025" in col_str or "+1y" in col_str.lower():
                        avg_val = rev_est.at["avg", col] if "avg" in rev_est.index else None
                        data["revenue_2025e"] = millions(avg_val)
                    elif "2026" in col_str or "+2y" in col_str.lower():
                        avg_val = rev_est.at["avg", col] if "avg" in rev_est.index else None
                        data["revenue_2026e"] = millions(avg_val)
        except Exception as e:
            print(f"    [WARN] Revenue estimate error for {ticker_str}: {e}")

        # Earnings estimates for forward PE
        try:
            earn_est = tk.earnings_estimate
            if earn_est is not None and not earn_est.empty:
                for col in earn_est.columns:
                    col_str = str(col)
                    if "2025" in col_str or "+1y" in col_str.lower():
                        data["eps_2025e"] = earn_est.at["avg", col] if "avg" in earn_est.index else None
                    elif "2026" in col_str or "+2y" in col_str.lower():
                        data["eps_2026e"] = earn_est.at["avg", col] if "avg" in earn_est.index else None
        except Exception as e:
            print(f"    [WARN] Earnings estimate error for {ticker_str}: {e}")

        return data

    except Exception as e:
        print(f"    [ERROR] Failed to fetch {ticker_str}: {e}")
        return None


def fetch_price_history(ticker_str, period="1y"):
    """Fetch historical daily prices for indexed price series."""
    import yfinance as yf

    yf_ticker = YFINANCE_TICKER_OVERRIDES.get(ticker_str, ticker_str)
    try:
        tk = yf.Ticker(yf_ticker)
        hist = tk.history(period=period)
        if hist is not None and not hist.empty:
            prices = []
            for date, row in hist.iterrows():
                prices.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "close": round(row["Close"], 2)
                })
            return prices
    except Exception as e:
        print(f"    [WARN] Price history error for {ticker_str}: {e}")
    return None


# ---------------------------------------------------------------------------
#  Perplexity API for forward estimates (optional)
# ---------------------------------------------------------------------------

def perplexity_query(prompt, api_key, system_prompt=None):
    """Query Perplexity Sonar API."""
    url = "https://api.perplexity.ai/chat/completions"
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    payload = json.dumps({
        "model": "sonar-pro",
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 4000,
    }).encode("utf-8")

    req = urllib.request.Request(url, data=payload, headers={
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    })

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"  [ERROR] Perplexity API error: {e}")
        return None


def fetch_perplexity_estimates(companies_needing_estimates, api_key):
    """Use Perplexity to fill in forward estimates for companies missing them."""
    if not api_key or not companies_needing_estimates:
        return {}

    results = {}
    batch_size = 5

    for i in range(0, len(companies_needing_estimates), batch_size):
        batch = companies_needing_estimates[i:i+batch_size]
        tickers = [c["display_ticker"] for c in batch]
        names = [f'{c["display_ticker"]} ({c["name"]})' for c in batch]

        print(f"\n  [Perplexity] Fetching estimates for: {', '.join(tickers)}")

        prompt = f"""For these companies, provide consensus analyst estimates in JSON format.
Companies: {', '.join(names)}

Return a JSON array where each item has:
- "ticker_id": the ticker symbol
- "revenue_2025e_millions": FY2025 consensus revenue estimate in USD millions (number or null)
- "revenue_2026e_millions": FY2026 consensus revenue estimate in USD millions (number or null)
- "ebitda_2025e_millions": FY2025 consensus EBITDA estimate in USD millions (number or null)
- "ebitda_2026e_millions": FY2026 consensus EBITDA estimate in USD millions (number or null)
- "net_income_2025e_millions": FY2025 consensus net income estimate in USD millions (number or null)
- "net_income_2026e_millions": FY2026 consensus net income estimate in USD millions (number or null)

Return ONLY the JSON array. No markdown, no explanations."""

        response = perplexity_query(prompt, api_key,
            system_prompt="You are a financial data assistant. Return only valid JSON arrays with numeric values in USD millions.")

        if response:
            try:
                # Extract JSON from response
                import re
                json_match = re.search(r'\[.*\]', response, re.DOTALL)
                if json_match:
                    data = json.loads(json_match.group())
                    for item in data:
                        tid = item.get("ticker_id", "")
                        results[tid] = item
                        print(f"    Got estimates for {tid}")
            except (json.JSONDecodeError, Exception) as e:
                print(f"    [WARN] Failed to parse Perplexity response: {e}")

        time.sleep(2)  # Rate limiting

    return results


# ---------------------------------------------------------------------------
#  Data Processing
# ---------------------------------------------------------------------------

def process_company(yf_data, company_meta, perplexity_estimates=None):
    """Transform yfinance data into dashboard format."""
    result = {
        "ticker": company_meta["display_ticker"],
        "fmp_ticker": company_meta["ticker"],
        "name": company_meta["name"],
        "category": company_meta["category"],
    }

    if yf_data is None:
        return result

    # Current price metrics
    result["share_price"] = yf_data.get("share_price")
    result["market_cap"] = yf_data.get("market_cap")
    result["enterprise_value"] = yf_data.get("enterprise_value")

    # 52-week metrics
    high = yf_data.get("year_high")
    low = yf_data.get("year_low")
    price = result.get("share_price")
    result["year_high"] = high
    result["year_low"] = low
    if price and high and high > 0:
        result["pct_52wk"] = round((price / high) * 100, 2)

    # Revenue data
    rev_2023 = yf_data.get("revenue_2023")
    rev_2024 = yf_data.get("revenue_2024")
    rev_2025e = yf_data.get("revenue_2025e")
    rev_2026e = yf_data.get("revenue_2026e")

    # Fill in from Perplexity estimates if available
    pplx = perplexity_estimates or {}
    ticker_id = company_meta["display_ticker"]
    if ticker_id in pplx:
        est = pplx[ticker_id]
        if rev_2025e is None:
            rev_2025e = est.get("revenue_2025e_millions")
        if rev_2026e is None:
            rev_2026e = est.get("revenue_2026e_millions")

    result["revenue_2024"] = rev_2024
    result["revenue_2025e"] = rev_2025e
    result["revenue_2026e"] = rev_2026e

    # Revenue growth
    result["rev_growth_2024"] = safe_div(rev_2024 - rev_2023, rev_2023) if rev_2024 and rev_2023 else None
    result["rev_growth_2025"] = safe_div(rev_2025e - rev_2024, rev_2024) if rev_2025e and rev_2024 else None

    # EBITDA data
    ebitda_2024 = yf_data.get("ebitda_2024")
    ebitda_2025e = None
    ebitda_2026e = None

    if ticker_id in pplx:
        est = pplx[ticker_id]
        ebitda_2025e = est.get("ebitda_2025e_millions")
        ebitda_2026e = est.get("ebitda_2026e_millions")

    result["ebitda_2024"] = ebitda_2024
    result["ebitda_2025e"] = ebitda_2025e
    result["ebitda_2026e"] = ebitda_2026e

    # EBITDA margins
    result["ebitda_margin_2024"] = safe_div(ebitda_2024, rev_2024)
    result["ebitda_margin_2025"] = safe_div(ebitda_2025e, rev_2025e)
    result["ebitda_margin_2026"] = safe_div(ebitda_2026e, rev_2026e)

    # EV multiples
    ev = result.get("enterprise_value")
    result["ev_rev_2024"] = safe_div(ev, rev_2024)
    result["ev_rev_2025"] = safe_div(ev, rev_2025e)
    result["ev_rev_2026"] = safe_div(ev, rev_2026e)
    result["ev_ebitda_2024"] = safe_div(ev, ebitda_2024)
    result["ev_ebitda_2025"] = safe_div(ev, ebitda_2025e)
    result["ev_ebitda_2026"] = safe_div(ev, ebitda_2026e)

    # PE ratios
    ni_2024 = yf_data.get("net_income_2024")
    mcap = result.get("market_cap")
    result["pe_2024"] = safe_div(mcap, ni_2024)

    # Forward PE from share price and EPS estimates
    eps_2025 = yf_data.get("eps_2025e")
    eps_2026 = yf_data.get("eps_2026e")
    if price and eps_2025 and eps_2025 > 0:
        result["pe_2025"] = round(price / eps_2025, 2)
    elif ticker_id in pplx:
        ni_2025e = pplx[ticker_id].get("net_income_2025e_millions")
        result["pe_2025"] = safe_div(mcap, ni_2025e)

    if price and eps_2026 and eps_2026 > 0:
        result["pe_2026"] = round(price / eps_2026, 2)
    elif ticker_id in pplx:
        ni_2026e = pplx[ticker_id].get("net_income_2026e_millions")
        result["pe_2026"] = safe_div(mcap, ni_2026e)

    return result


def validate_company(company_data):
    """Check data quality for a single company."""
    issues = []
    if not company_data.get("share_price"):
        issues.append("no share_price")
    if not company_data.get("market_cap"):
        issues.append("no market_cap")
    if not company_data.get("enterprise_value"):
        issues.append("no enterprise_value")
    if not company_data.get("revenue_2024") and not company_data.get("revenue_2025e"):
        issues.append("no revenue data")

    margin = company_data.get("ebitda_margin_2024")
    if margin is not None and (margin < -1.0 or margin > 0.8):
        issues.append(f"suspicious EBITDA margin: {margin:.2%}")

    return len(issues) == 0, issues


def compute_category_averages(companies_data, categories):
    """Compute mean/median for key metrics by category."""
    metrics = [
        "rev_growth_2024", "rev_growth_2025",
        "ebitda_margin_2024", "ebitda_margin_2025", "ebitda_margin_2026",
        "ev_rev_2024", "ev_rev_2025", "ev_rev_2026",
        "ev_ebitda_2024", "ev_ebitda_2025", "ev_ebitda_2026",
        "pe_2024", "pe_2025", "pe_2026",
    ]

    summaries = {}
    for cat in categories + ["Overall"]:
        if cat == "Overall":
            group = companies_data
        else:
            group = [c for c in companies_data if c.get("category") == cat]

        mean_vals = {}
        median_vals = {}
        for metric in metrics:
            values = [c[metric] for c in group if c.get(metric) is not None and c[metric] > 0]
            if values:
                mean_vals[metric] = round(statistics.mean(values), 6)
                median_vals[metric] = round(statistics.median(values), 6)

        summaries[cat] = {"mean": mean_vals, "median": median_vals}

    return summaries


def build_price_series(companies):
    """Build indexed price series for each category."""
    import yfinance as yf

    print("\nFetching price history for indexed series...")
    series = {}

    for cat in CATEGORIES:
        cat_companies = [c for c in companies if c["category"] == cat]
        cat_prices = {}

        for comp in cat_companies:
            ticker = comp["ticker"]
            yf_ticker = YFINANCE_TICKER_OVERRIDES.get(ticker, ticker)
            try:
                tk = yf.Ticker(yf_ticker)
                hist = tk.history(period="1y")
                if hist is not None and not hist.empty:
                    first_close = hist["Close"].iloc[0]
                    for date, row in hist.iterrows():
                        date_str = date.strftime("%Y-%m-%d")
                        indexed = (row["Close"] / first_close) * 100
                        if date_str not in cat_prices:
                            cat_prices[date_str] = []
                        cat_prices[date_str].append(indexed)
                time.sleep(0.3)
            except Exception as e:
                print(f"  [WARN] Price history error for {ticker}: {e}")

        # Average the indexed prices for each date
        series[cat] = {}
        for date_str in sorted(cat_prices.keys()):
            vals = cat_prices[date_str]
            series[cat][date_str] = round(statistics.mean(vals), 2)

    return series


# ---------------------------------------------------------------------------
#  Main
# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="CPMA Public Comps Data Feed")
    parser.add_argument("--api-key", default=os.environ.get("PERPLEXITY_API_KEY", ""),
                        help="Perplexity API key for forward estimates")
    parser.add_argument("--output", default="cpma_comps_data.json",
                        help="Output JSON file path")
    parser.add_argument("--no-perplexity", action="store_true",
                        help="Skip Perplexity API calls (yfinance only)")
    parser.add_argument("--no-price-series", action="store_true",
                        help="Skip building price series (faster for testing)")
    args = parser.parse_args()

    output_path = Path(args.output)
    api_key = args.api_key if not args.no_perplexity else ""

    print("=" * 60)
    print("CPMA Public Comps Data Feed (yfinance + Perplexity)")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Companies: {len(COMPANIES)}")
    print(f"Perplexity estimates: {'enabled' if api_key else 'disabled'}")
    print("=" * 60)

    # Step 1: Fetch yfinance data for all companies
    print("\n--- Step 1: Fetching yfinance data ---")
    yf_data = {}
    for i, comp in enumerate(COMPANIES):
        ticker = comp["ticker"]
        print(f"\n  [{i+1}/{len(COMPANIES)}] {comp['display_ticker']} ({comp['name']})")
        yf_data[ticker] = fetch_yfinance_data(ticker)
        time.sleep(0.5)  # Rate limiting

    # Step 2: Identify companies needing forward estimates
    companies_needing_estimates = []
    for comp in COMPANIES:
        d = yf_data.get(comp["ticker"])
        if d and not d.get("revenue_2025e"):
            companies_needing_estimates.append(comp)

    print(f"\n--- Step 2: Forward estimates needed for {len(companies_needing_estimates)}/{len(COMPANIES)} companies ---")

    # Step 3: Fetch Perplexity estimates if needed
    perplexity_estimates = {}
    if api_key and companies_needing_estimates:
        perplexity_estimates = fetch_perplexity_estimates(companies_needing_estimates, api_key)
        print(f"  Got Perplexity estimates for {len(perplexity_estimates)} companies")
    elif not api_key:
        print("  Perplexity disabled, skipping forward estimates")

    # Step 4: Process all companies
    print("\n--- Step 3: Processing companies ---")
    companies_data = []
    for comp in COMPANIES:
        raw = yf_data.get(comp["ticker"])
        processed = process_company(raw, comp, perplexity_estimates)
        valid, issues = validate_company(processed)
        if issues:
            print(f"  [WARN] {comp['display_ticker']}: {', '.join(issues)}")
        companies_data.append(processed)

    # Step 5: Data quality check
    has_price = sum(1 for c in companies_data if c.get("share_price"))
    has_ev = sum(1 for c in companies_data if c.get("enterprise_value"))
    has_revenue = sum(1 for c in companies_data if c.get("revenue_2024") or c.get("revenue_2025e"))

    print(f"\n{'='*60}")
    print("Data Quality Report:")
    print(f"  Companies with price:    {has_price}/{len(COMPANIES)}")
    print(f"  Companies with EV:       {has_ev}/{len(COMPANIES)}")
    print(f"  Companies with revenue:  {has_revenue}/{len(COMPANIES)}")

    # Quality gates
    MIN_PRICE_THRESHOLD = 30
    MIN_REVENUE_THRESHOLD = 15  # Lower threshold since forward estimates may be limited

    if has_price < MIN_PRICE_THRESHOLD:
        print(f"\n[ABORT] Only {has_price} companies have price data (need {MIN_PRICE_THRESHOLD}+).")
        print("Aborting to prevent dashboard corruption.")
        sys.exit(1)

    if has_revenue < MIN_REVENUE_THRESHOLD:
        print(f"\n[ABORT] Only {has_revenue} companies have revenue data (need {MIN_REVENUE_THRESHOLD}+).")
        print("Aborting to prevent dashboard corruption.")
        sys.exit(1)

    # Step 6: Compute category summaries
    print("\nComputing category summaries...")
    summaries = compute_category_averages(companies_data, CATEGORIES)

    # Step 7: Build price series (optional)
    price_series = {}
    if not args.no_price_series:
        price_series = build_price_series(COMPANIES)
    else:
        print("\nSkipping price series (--no-price-series)")

    # Step 8: Build output
    output = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "source": "yfinance (Yahoo Finance) + Perplexity Sonar API",
            "num_companies": len(companies_data),
            "categories": CATEGORIES,
        },
        "companies": companies_data,
        "category_summaries": summaries,
        "category_price_series": price_series,
    }

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\nData saved to: {output_path}")
    print(f"Companies processed: {len(companies_data)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
