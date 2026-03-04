#!/usr/bin/env python3
"""
CPMA Public Comps Data Feed (yfinance + Perplexity Sonar API)
=============================================================
Fetches real-time market data for Engineering & Construction public comparables.
Uses yfinance for price/fundamentals and optionally Perplexity for forward estimates.

Fiscal year windows are determined dynamically from the current date:
  LFY = last completed fiscal year (actuals)
  CFY = current fiscal year (estimates)
  NFY = next fiscal year (estimates)

Usage:
    python cpma_data_feed_yfinance.py                       # Uses PERPLEXITY_API_KEY env var
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
#  Dynamic Fiscal Year Windows
# ---------------------------------------------------------------------------
_now = datetime.now()
LFY = _now.year - 1   # Last Fiscal Year (actuals)  — e.g. 2025
CFY = _now.year        # Current Fiscal Year (estimates) — e.g. 2026
NFY = _now.year + 1    # Next Fiscal Year (estimates) — e.g. 2027

# ---------------------------------------------------------------------------
#  Company Universe
# ---------------------------------------------------------------------------

COMPANIES = [
    # Construction Contractors
    {"ticker": "ACS.MC",    "name": "ACS, Actividades de Construccion y Servicios", "category": "Construction Contractors",                     "display_ticker": "ACS"},
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
        # Dynamically look for LFY (e.g. 2025) and LFY-1 (e.g. 2024) data
        try:
            inc = tk.income_stmt
            if inc is not None and not inc.empty:
                cols = sorted(inc.columns, reverse=True)

                for col in cols:
                    year = col.year if hasattr(col, 'year') else None
                    if year == LFY:
                        data["revenue_lfy"] = millions(inc.at["Total Revenue", col]) if "Total Revenue" in inc.index else None
                        data["ebitda_lfy"] = millions(inc.at["EBITDA", col]) if "EBITDA" in inc.index else None
                        data["net_income_lfy"] = millions(inc.at["Net Income", col]) if "Net Income" in inc.index else None
                    elif year == LFY - 1:
                        data["revenue_lfy_minus1"] = millions(inc.at["Total Revenue", col]) if "Total Revenue" in inc.index else None

                # Fallback: if LFY not available, try LFY-1 as the "actual" year
                # (some companies may not have filed their latest annual yet)
                if not data.get("revenue_lfy") and data.get("revenue_lfy_minus1"):
                    # Try to find LFY-1 full data as fallback actuals
                    for col in cols:
                        year = col.year if hasattr(col, 'year') else None
                        if year == LFY - 1:
                            data["revenue_lfy"] = millions(inc.at["Total Revenue", col]) if "Total Revenue" in inc.index else None
                            data["ebitda_lfy"] = millions(inc.at["EBITDA", col]) if "EBITDA" in inc.index else None
                            data["net_income_lfy"] = millions(inc.at["Net Income", col]) if "Net Income" in inc.index else None
                        elif year == LFY - 2:
                            data["revenue_lfy_minus1"] = millions(inc.at["Total Revenue", col]) if "Total Revenue" in inc.index else None
                            break
                    print(f"    [INFO] Using FY{LFY-1} as fallback actuals (FY{LFY} not yet available)")
        except Exception as e:
            print(f"    [WARN] Income statement error for {ticker_str}: {e}")

        # Forward estimates from analyst data
        # Look for CFY (current fiscal year) and NFY (next fiscal year) estimates
        try:
            rev_est = tk.revenue_estimate
            if rev_est is not None and not rev_est.empty:
                print(f"    [DEBUG] Revenue estimate columns: {list(rev_est.columns)}")
                for col in rev_est.columns:
                    col_str = str(col)
                    # Match by year number or relative year labels
                    if str(CFY) in col_str or "0y" in col_str.lower():
                        avg_val = rev_est.at["avg", col] if "avg" in rev_est.index else None
                        data["revenue_cfy_e"] = millions(avg_val)
                        print(f"    [DEBUG] Matched CFY revenue ({col_str}): {data['revenue_cfy_e']}")
                    elif str(NFY) in col_str or "+1y" in col_str.lower():
                        avg_val = rev_est.at["avg", col] if "avg" in rev_est.index else None
                        data["revenue_nfy_e"] = millions(avg_val)
                        print(f"    [DEBUG] Matched NFY revenue ({col_str}): {data['revenue_nfy_e']}")
        except Exception as e:
            print(f"    [WARN] Revenue estimate error for {ticker_str}: {e}")

        # Earnings estimates for forward PE
        try:
            earn_est = tk.earnings_estimate
            if earn_est is not None and not earn_est.empty:
                print(f"    [DEBUG] Earnings estimate columns: {list(earn_est.columns)}")
                for col in earn_est.columns:
                    col_str = str(col)
                    if str(CFY) in col_str or "0y" in col_str.lower():
                        data["eps_cfy_e"] = earn_est.at["avg", col] if "avg" in earn_est.index else None
                    elif str(NFY) in col_str or "+1y" in col_str.lower():
                        data["eps_nfy_e"] = earn_est.at["avg", col] if "avg" in earn_est.index else None
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
    """Use Perplexity to fill in forward estimates for companies missing them.

    Asks for CFY and NFY estimates (dynamically determined from current date).
    """
    if not api_key or not companies_needing_estimates:
        return {}

    results = {}
    batch_size = 5

    for i in range(0, len(companies_needing_estimates), batch_size):
        batch = companies_needing_estimates[i:i+batch_size]
        tickers = [c["display_ticker"] for c in batch]
        names = [f'{c["display_ticker"]} ({c["name"]})' for c in batch]

        print(f"\n  [Perplexity] Fetching FY{CFY}/FY{NFY} estimates for: {', '.join(tickers)}")

        prompt = f"""For these engineering & construction companies, provide consensus Wall Street analyst estimates in JSON format.
Companies: {', '.join(names)}

Return a JSON array where each item has:
- "ticker_id": the ticker symbol exactly as shown above (e.g. "ACS", "SKA.B", "DG", etc.)
- "revenue_{CFY}e_millions": FY{CFY} consensus revenue estimate in USD millions (number or null)
- "revenue_{NFY}e_millions": FY{NFY} consensus revenue estimate in USD millions (number or null)
- "ebitda_{CFY}e_millions": FY{CFY} consensus EBITDA estimate in USD millions (number or null)
- "ebitda_{NFY}e_millions": FY{NFY} consensus EBITDA estimate in USD millions (number or null)
- "net_income_{CFY}e_millions": FY{CFY} consensus net income estimate in USD millions (number or null)
- "net_income_{NFY}e_millions": FY{NFY} consensus net income estimate in USD millions (number or null)

Important: For non-US companies, convert estimates to USD millions using current exchange rates.
Return ONLY the JSON array. No markdown, no explanations."""

        response = perplexity_query(prompt, api_key,
            system_prompt="You are a financial data assistant. Return only valid JSON arrays with numeric values in USD millions. Use current exchange rates for non-USD companies.")

        if response:
            try:
                import re
                json_match = re.search(r'\[.*\]', response, re.DOTALL)
                if json_match:
                    data = json.loads(json_match.group())
                    for item in data:
                        tid = item.get("ticker_id", "").strip()
                        # Normalize ticker matching — try exact match first,
                        # then try matching without exchange suffixes
                        matched_ticker = None
                        for expected in tickers:
                            if tid.upper() == expected.upper():
                                matched_ticker = expected
                                break
                            # Try partial match (e.g. "ACS" matches "ACS")
                            if tid.upper().split(".")[0] == expected.upper().split(".")[0]:
                                matched_ticker = expected
                                break
                        if matched_ticker:
                            results[matched_ticker] = item
                            print(f"    Got estimates for {matched_ticker} (from response ticker: {tid})")
                        else:
                            print(f"    [WARN] Could not match Perplexity ticker '{tid}' to any expected ticker")
            except (json.JSONDecodeError, Exception) as e:
                print(f"    [WARN] Failed to parse Perplexity response: {e}")
                print(f"    Response was: {response[:200]}...")

        time.sleep(2)  # Rate limiting

    return results


# ---------------------------------------------------------------------------
#  Data Processing
# ---------------------------------------------------------------------------

def process_company(yf_data, company_meta, perplexity_estimates=None):
    """Transform yfinance data into dashboard format.

    Output field names use actual year numbers (e.g. rev_growth_2025, ev_rev_2026)
    based on the dynamic LFY/CFY/NFY constants.
    """
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
    rev_lfy_minus1 = yf_data.get("revenue_lfy_minus1")
    rev_lfy = yf_data.get("revenue_lfy")
    rev_cfy_e = yf_data.get("revenue_cfy_e")
    rev_nfy_e = yf_data.get("revenue_nfy_e")

    # Fill in from Perplexity estimates if available
    pplx = perplexity_estimates or {}
    ticker_id = company_meta["display_ticker"]
    if ticker_id in pplx:
        est = pplx[ticker_id]
        if rev_cfy_e is None:
            rev_cfy_e = est.get(f"revenue_{CFY}e_millions")
        if rev_nfy_e is None:
            rev_nfy_e = est.get(f"revenue_{NFY}e_millions")

    # Output with actual year numbers
    result[f"revenue_{LFY}"] = rev_lfy
    result[f"revenue_{CFY}e"] = rev_cfy_e
    result[f"revenue_{NFY}e"] = rev_nfy_e

    # Revenue growth
    result[f"rev_growth_{LFY}"] = safe_div(rev_lfy - rev_lfy_minus1, rev_lfy_minus1) if rev_lfy and rev_lfy_minus1 else None
    result[f"rev_growth_{CFY}"] = safe_div(rev_cfy_e - rev_lfy, rev_lfy) if rev_cfy_e and rev_lfy else None

    # EBITDA data
    ebitda_lfy = yf_data.get("ebitda_lfy")
    ebitda_cfy_e = None
    ebitda_nfy_e = None

    if ticker_id in pplx:
        est = pplx[ticker_id]
        ebitda_cfy_e = est.get(f"ebitda_{CFY}e_millions")
        ebitda_nfy_e = est.get(f"ebitda_{NFY}e_millions")

    result[f"ebitda_{LFY}"] = ebitda_lfy
    result[f"ebitda_{CFY}e"] = ebitda_cfy_e
    result[f"ebitda_{NFY}e"] = ebitda_nfy_e

    # EBITDA margins
    result[f"ebitda_margin_{LFY}"] = safe_div(ebitda_lfy, rev_lfy)
    result[f"ebitda_margin_{CFY}"] = safe_div(ebitda_cfy_e, rev_cfy_e)
    result[f"ebitda_margin_{NFY}"] = safe_div(ebitda_nfy_e, rev_nfy_e)

    # EV multiples
    ev = result.get("enterprise_value")
    result[f"ev_rev_{LFY}"] = safe_div(ev, rev_lfy)
    result[f"ev_rev_{CFY}"] = safe_div(ev, rev_cfy_e)
    result[f"ev_rev_{NFY}"] = safe_div(ev, rev_nfy_e)
    result[f"ev_ebitda_{LFY}"] = safe_div(ev, ebitda_lfy)
    result[f"ev_ebitda_{CFY}"] = safe_div(ev, ebitda_cfy_e)
    result[f"ev_ebitda_{NFY}"] = safe_div(ev, ebitda_nfy_e)

    # PE ratios
    ni_lfy = yf_data.get("net_income_lfy")
    mcap = result.get("market_cap")
    result[f"pe_{LFY}"] = safe_div(mcap, ni_lfy)

    # Forward PE from share price and EPS estimates
    eps_cfy = yf_data.get("eps_cfy_e")
    eps_nfy = yf_data.get("eps_nfy_e")
    if price and eps_cfy and eps_cfy > 0:
        result[f"pe_{CFY}"] = round(price / eps_cfy, 2)
    elif ticker_id in pplx:
        ni_cfy_e = pplx[ticker_id].get(f"net_income_{CFY}e_millions")
        result[f"pe_{CFY}"] = safe_div(mcap, ni_cfy_e)

    if price and eps_nfy and eps_nfy > 0:
        result[f"pe_{NFY}"] = round(price / eps_nfy, 2)
    elif ticker_id in pplx:
        ni_nfy_e = pplx[ticker_id].get(f"net_income_{NFY}e_millions")
        result[f"pe_{NFY}"] = safe_div(mcap, ni_nfy_e)

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
    if not company_data.get(f"revenue_{LFY}") and not company_data.get(f"revenue_{CFY}e"):
        issues.append("no revenue data")

    margin = company_data.get(f"ebitda_margin_{LFY}")
    if margin is not None and (margin < -1.0 or margin > 0.8):
        issues.append(f"suspicious EBITDA margin: {margin:.2%}")

    return len(issues) == 0, issues


def compute_category_averages(companies_data, categories):
    """Compute mean/median for key metrics by category."""
    metrics = [
        f"rev_growth_{LFY}", f"rev_growth_{CFY}",
        f"ebitda_margin_{LFY}", f"ebitda_margin_{CFY}", f"ebitda_margin_{NFY}",
        f"ev_rev_{LFY}", f"ev_rev_{CFY}", f"ev_rev_{NFY}",
        f"ev_ebitda_{LFY}", f"ev_ebitda_{CFY}", f"ev_ebitda_{NFY}",
        f"pe_{LFY}", f"pe_{CFY}", f"pe_{NFY}",
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


def _snap_to_monday(date_str):
    """Snap a YYYY-MM-DD date string to the Monday of that week."""
    from datetime import timedelta
    d = datetime.strptime(date_str, "%Y-%m-%d")
    monday = d - timedelta(days=d.weekday())  # weekday(): Mon=0 … Sun=6
    return monday.strftime("%Y-%m-%d")


def _weekly_indexed(hist_df):
    """Convert a daily yfinance history DataFrame into weekly indexed data.

    Returns a dict {monday_date_str: last_close_of_that_week} so that each
    trading week is represented exactly once.  The caller normalises to
    index-100 after collecting all companies.
    """
    weekly = {}  # monday_str -> last close price seen that week
    for date, row in hist_df.iterrows():
        monday = _snap_to_monday(date.strftime("%Y-%m-%d"))
        weekly[monday] = row["Close"]  # keep overwriting → last day of week wins
    return weekly


def build_price_series(companies):
    """Build indexed price series for each category AND per-company price history.

    All series are snapped to weekly (Monday) buckets so every category and
    company shares the same x-axis dates — required because the dashboard
    chart uses Chart.js ``type:"category"`` labels from the first dataset.

    Returns:
        (category_price_series, price_history)
        - category_price_series: {category_name: [{date, indexed}, ...], "S&P 500": [{date, indexed}, ...]}
        - price_history: {display_ticker: [{date, indexed}, ...]}
    """
    import yfinance as yf

    print("\nFetching price history for indexed series...")
    category_series = {}
    price_history = {}

    # Collect a master set of Monday dates across ALL companies so every
    # series uses the same x-axis labels.
    master_mondays = set()

    # --- Per-company raw weekly closes ---
    company_weekly = {}  # display_ticker -> {monday_str: close}
    company_cat = {}     # display_ticker -> category

    for comp in companies:
        ticker = comp["ticker"]
        display_ticker = comp["display_ticker"]
        yf_ticker = YFINANCE_TICKER_OVERRIDES.get(ticker, ticker)
        try:
            tk = yf.Ticker(yf_ticker)
            hist = tk.history(period="1y")
            if hist is not None and not hist.empty:
                weekly = _weekly_indexed(hist)
                company_weekly[display_ticker] = weekly
                company_cat[display_ticker] = comp["category"]
                master_mondays.update(weekly.keys())
            time.sleep(0.3)
        except Exception as e:
            print(f"  [WARN] Price history error for {ticker}: {e}")

    # Sort master Mondays once
    sorted_mondays = sorted(master_mondays)

    # --- Build per-company indexed series (index 100 at first available week) ---
    for dticker, weekly in company_weekly.items():
        first_close = None
        series = []
        for monday in sorted_mondays:
            if monday in weekly:
                if first_close is None:
                    first_close = weekly[monday]
                indexed = round((weekly[monday] / first_close) * 100, 2)
                series.append({"date": monday, "indexed": indexed})
            elif first_close is not None and series:
                # Carry forward last value for weeks where this company
                # had no trading day (holiday, etc.)
                series.append({"date": monday, "indexed": series[-1]["indexed"]})
        price_history[dticker] = series

    # --- Build category averages from per-company series ---
    for cat in CATEGORIES:
        cat_tickers = [dt for dt, c in company_cat.items() if c == cat]
        if not cat_tickers:
            continue
        cat_series = []
        for monday in sorted_mondays:
            vals = []
            for dt in cat_tickers:
                for pt in price_history.get(dt, []):
                    if pt["date"] == monday:
                        vals.append(pt["indexed"])
                        break
            if vals:
                cat_series.append({"date": monday, "indexed": round(statistics.mean(vals), 2)})
        category_series[cat] = cat_series

    # --- S&P 500 benchmark (same weekly grid) ---
    print("  Fetching S&P 500 benchmark (^GSPC)...")
    try:
        sp = yf.Ticker("^GSPC")
        hist = sp.history(period="1y")
        if hist is not None and not hist.empty:
            sp_weekly = _weekly_indexed(hist)
            first_close = None
            sp_series = []
            for monday in sorted_mondays:
                if monday in sp_weekly:
                    if first_close is None:
                        first_close = sp_weekly[monday]
                    indexed = round((sp_weekly[monday] / first_close) * 100, 2)
                    sp_series.append({"date": monday, "indexed": indexed})
                elif first_close is not None and sp_series:
                    sp_series.append({"date": monday, "indexed": sp_series[-1]["indexed"]})
            category_series["S&P 500"] = sp_series
            print(f"    Got {len(sp_series)} data points for S&P 500")
    except Exception as e:
        print(f"  [WARN] S&P 500 fetch error: {e}")

    print(f"  Weekly dates: {len(sorted_mondays)} Mondays")
    return category_series, price_history


# ---------------------------------------------------------------------------
#  Main
# ---------------------------------------------------------------------------

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
    print(f"Fiscal year windows: LFY={LFY}A, CFY={CFY}E, NFY={NFY}E")
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
        if d and not d.get("revenue_cfy_e"):
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
    has_revenue = sum(1 for c in companies_data if c.get(f"revenue_{LFY}") or c.get(f"revenue_{CFY}e"))

    print(f"\n{'='*60}")
    print("Data Quality Report:")
    print(f"  Companies with price:    {has_price}/{len(COMPANIES)}")
    print(f"  Companies with EV:       {has_ev}/{len(COMPANIES)}")
    print(f"  Companies with revenue:  {has_revenue}/{len(COMPANIES)}")

    # Detailed forward estimate coverage
    has_rev_cfy = sum(1 for c in companies_data if c.get(f"revenue_{CFY}e"))
    has_rev_nfy = sum(1 for c in companies_data if c.get(f"revenue_{NFY}e"))
    has_ebitda_cfy = sum(1 for c in companies_data if c.get(f"ebitda_{CFY}e"))
    has_pe_cfy = sum(1 for c in companies_data if c.get(f"pe_{CFY}"))
    print(f"  Revenue {CFY}E coverage: {has_rev_cfy}/{len(COMPANIES)}")
    print(f"  Revenue {NFY}E coverage: {has_rev_nfy}/{len(COMPANIES)}")
    print(f"  EBITDA {CFY}E coverage:  {has_ebitda_cfy}/{len(COMPANIES)}")
    print(f"  PE {CFY}E coverage:      {has_pe_cfy}/{len(COMPANIES)}")

    # Quality gates
    MIN_PRICE_THRESHOLD = 30
    MIN_REVENUE_THRESHOLD = 15

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
    category_price_series = {}
    price_history = {}
    if not args.no_price_series:
        category_price_series, price_history = build_price_series(COMPANIES)
    else:
        print("\nSkipping price series (--no-price-series)")

    # Step 8: Build output
    output = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "source": "yfinance (Yahoo Finance) + Perplexity Sonar API",
            "num_companies": len(companies_data),
            "categories": CATEGORIES,
            "fiscal_years": {
                "lfy": LFY,
                "cfy": CFY,
                "nfy": NFY,
            },
        },
        "companies": companies_data,
        "category_summaries": summaries,
        "category_price_series": category_price_series,
        "price_history": price_history,
    }

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\nData saved to: {output_path}")
    print(f"Companies processed: {len(companies_data)}")
    print("=" * 60)


if __name__ == "__main__":
    main()

