#!/usr/bin/env python3
"""
CPMA Public Comps Data Feed (Perplexity Sonar API)
===================================================
Fetches real-time market data for Engineering & Construction public comparables.
Uses Perplexity Sonar API with access to Fiscal.ai and Morningstar data.

Usage:
    python cpma_data_feed_perplexity.py                          # Uses PERPLEXITY_API_KEY env var
    python cpma_data_feed_perplexity.py --api-key YOUR_KEY       # Uses provided key
    python cpma_data_feed_perplexity.py --output data.json       # Custom output path

Output: cpma_comps_data.json (consumed by the HTML dashboard)
"""

import json
import os
import re
import sys
import time
import argparse
import statistics
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path


# âââ Company Universe âââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

COMPANIES = [
    # Construction Contractors
    {"ticker": "ACS.MC",    "name": "ACS, Actividades de Construccion y Servicios", "category": "Construction Contractors",                    "display_ticker": "ACS"},
    {"ticker": "SKA-B.ST",  "name": "Skanska",                                     "category": "Construction Contractors",                    "display_ticker": "SKA.B"},
    {"ticker": "AGX",       "name": "Argan",                                        "category": "Construction Contractors",                    "display_ticker": "AGX"},
    {"ticker": "TPC",       "name": "Tutor Perini",                                 "category": "Construction Contractors",                    "display_ticker": "TPC"},
    {"ticker": "ARE.TO",    "name": "Aecon Group",                                  "category": "Construction Contractors",                    "display_ticker": "ARE"},
    {"ticker": "BWMN",      "name": "Bowman Consulting",                            "category": "Construction Contractors",                    "display_ticker": "BWMN"},

    # Diversified Engineering and Construction
    {"ticker": "DG.PA",     "name": "VINCI",            "category": "Diversified Engineering and Construction", "display_ticker": "DG"},
    {"ticker": "HOT.DE",    "name": "HOCHTIEF",         "category": "Diversified Engineering and Construction", "display_ticker": "HOT"},
    {"ticker": "EN.PA",     "name": "Bouygues",         "category": "Diversified Engineering and Construction", "display_ticker": "EN"},
    {"ticker": "J",         "name": "Jacobs Solutions",  "category": "Diversified Engineering and Construction", "display_ticker": "J"},
    {"ticker": "1802.T",    "name": "Obayashi",         "category": "Diversified Engineering and Construction", "display_ticker": "1802"},
    {"ticker": "ACM",       "name": "AECOM",            "category": "Diversified Engineering and Construction", "display_ticker": "ACM"},
    {"ticker": "STN.TO",    "name": "Stantec",          "category": "Diversified Engineering and Construction", "display_ticker": "STN"},
    {"ticker": "ATRL.TO",   "name": "Atkinsrealis",     "category": "Diversified Engineering and Construction", "display_ticker": "ATRL"},
    {"ticker": "KBR",       "name": "KBR",              "category": "Diversified Engineering and Construction", "display_ticker": "KBR"},
    {"ticker": "ARCAD.AS",  "name": "Arcadis",          "category": "Diversified Engineering and Construction", "display_ticker": "ARCAD"},
    {"ticker": "WBD.MI",    "name": "Webuild",          "category": "Diversified Engineering and Construction", "display_ticker": "WBD"},
    {"ticker": "FLR",       "name": "Fluor",            "category": "Diversified Engineering and Construction", "display_ticker": "FLR"},

    # Specialty Engineering and Construction
    {"ticker": "FIX",       "name": "Comfort Systems USA",  "category": "Specialty Engineering and Construction", "display_ticker": "FIX"},
    {"ticker": "EME",       "name": "EMCOR Group",          "category": "Specialty Engineering and Construction", "display_ticker": "EME"},
    {"ticker": "WSP.TO",    "name": "WSP Global",           "category": "Specialty Engineering and Construction", "display_ticker": "WSP"},
    {"ticker": "APG",       "name": "APi Group Corp",       "category": "Specialty Engineering and Construction", "display_ticker": "APG"},
    {"ticker": "DY",        "name": "Dycom Industries",     "category": "Specialty Engineering and Construction", "display_ticker": "DY"},
    {"ticker": "TTEK",      "name": "Tetra Tech",           "category": "Specialty Engineering and Construction", "display_ticker": "TTEK"},
    {"ticker": "GBF.DE",    "name": "Bilfinger",            "category": "Specialty Engineering and Construction", "display_ticker": "GBF"},

    # Infrastructure Services
    {"ticker": "STRL",      "name": "Sterling Infrastructure",   "category": "Infrastructure Services", "display_ticker": "STRL"},
    {"ticker": "ROAD",      "name": "Construction Partners",     "category": "Infrastructure Services", "display_ticker": "ROAD"},
    {"ticker": "GVA",       "name": "Granite Construction",      "category": "Infrastructure Services", "display_ticker": "GVA"},
    {"ticker": "BBY.L",     "name": "Balfour Beatty",            "category": "Infrastructure Services", "display_ticker": "BBY"},

    # Utility Services
    {"ticker": "PWR",       "name": "Quanta Services",    "category": "Utility Services", "display_ticker": "PWR"},
    {"ticker": "MTZ",       "name": "MasTec",             "category": "Utility Services", "display_ticker": "MTZ"},
    {"ticker": "PRIM",      "name": "Primoris Services",  "category": "Utility Services", "display_ticker": "PRIM"},
    {"ticker": "MYRG",      "name": "MYR Group",          "category": "Utility Services", "display_ticker": "MYRG"},
    {"ticker": "AMRC",      "name": "Ameresco",           "category": "Utility Services", "display_ticker": "AMRC"},

    # Management Consulting
    {"ticker": "IBM",       "name": "IBM",                   "category": "Management Consulting", "display_ticker": "IBM"},
    {"ticker": "ACN",       "name": "Accenture",             "category": "Management Consulting", "display_ticker": "ACN"},
    {"ticker": "BAH",       "name": "Booz Allen Hamilton",   "category": "Management Consulting", "display_ticker": "BAH"},
    {"ticker": "FCN",       "name": "FTI Consulting",        "category": "Management Consulting", "display_ticker": "FCN"},
    {"ticker": "HURN",      "name": "Huron Consulting",      "category": "Management Consulting", "display_ticker": "HURN"},
]

CATEGORIES = [
    "Construction Contractors",
    "Diversified Engineering and Construction",
    "Specialty Engineering and Construction",
    "Infrastructure Services",
    "Utility Services",
    "Management Consulting",
]

PERPLEXITY_API_URL = "https://api.perplexity.ai/chat/completions"


# âââ API Helpers ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def perplexity_query(prompt, api_key, system_prompt=None, temperature=0.1):
    """Make a chat completion request to Perplexity Sonar API."""
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": "sonar-pro",
        "messages": messages,
        "temperature": temperature,
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        req = urllib.request.Request(
            PERPLEXITY_API_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            response = json.loads(resp.read().decode("utf-8"))
        content = response["choices"][0]["message"]["content"]
        return content
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        print(f"  [ERROR] Perplexity HTTP {e.code}: {body[:200]}")
        return None
    except Exception as e:
        print(f"  [ERROR] Perplexity API error: {e}")
        return None


def extract_json_from_response(text):
    """Extract JSON array or object from Perplexity response text."""
    if text is None:
        return None

    # Try to find JSON in code blocks first
    code_block = re.search(r"```(?:json)?\s*\n?([\s\S]*?)\n?```", text)
    if code_block:
        text = code_block.group(1).strip()

    # Try to find a JSON array
    arr_match = re.search(r"(\[[\s\S]*\])", text)
    if arr_match:
        try:
            return json.loads(arr_match.group(1))
        except json.JSONDecodeError:
            pass

    # Try to find a JSON object
    obj_match = re.search(r"(\{[\s\S]*\})", text)
    if obj_match:
        try:
            result = json.loads(obj_match.group(1))
            # Wrap single object in array
            return [result] if isinstance(result, dict) else result
        except json.JSONDecodeError:
            pass

    # Last resort: try parsing the whole text
    try:
        result = json.loads(text.strip())
        return [result] if isinstance(result, dict) else result
    except json.JSONDecodeError:
        return None


def fetch_batch(companies_batch, api_key):
    """Query Perplexity for financial data on a batch of companies."""
    company_lines = []
    for c in companies_batch:
        exchange_hint = ""
        if "." in c["ticker"]:
            parts = c["ticker"].split(".")
            exchange_hint = f" (exchange suffix: .{parts[-1]})"
        elif c["ticker"][0].isdigit():
            exchange_hint = f" (Tokyo Stock Exchange)"
        company_lines.append(
            f'- {c["display_ticker"]}: {c["name"]}{exchange_hint} [ticker_id: {c["ticker"]}]'
        )

    companies_list = "\n".join(company_lines)

    system_prompt = (
        "You are a financial data extraction system. You MUST return ONLY a valid JSON array "
        "with no additional text, no markdown formatting, no explanations. "
        "Use null for any data point you cannot find. All monetary values in USD millions."
    )

    user_prompt = f"""Look up the following public companies and return their current financial data.
Use the most recent available data from financial databases (Morningstar, Fiscal.ai, Yahoo Finance, etc).

Companies:
{companies_list}

Return a JSON array where each element has EXACTLY these fields:
{{
  "ticker_id": "the ticker_id from the list above",
  "share_price": current share price in local currency (number),
  "market_cap_millions": market capitalization in USD millions (number),
  "enterprise_value_millions": enterprise value in USD millions (number),
  "high_52wk": 52-week high price in local currency (number),
  "low_52wk": 52-week low price in local currency (number),
  "revenue_2024_millions": FY2024 actual revenue in USD millions (number),
  "revenue_2025e_millions": FY2025 estimated/consensus revenue in USD millions (number),
  "revenue_2026e_millions": FY2026 estimated/consensus revenue in USD millions (number),
  "ebitda_2024_millions": FY2024 actual EBITDA in USD millions (number),
  "ebitda_2025e_millions": FY2025 estimated/consensus EBITDA in USD millions (number),
  "ebitda_2026e_millions": FY2026 estimated/consensus EBITDA in USD millions (number),
  "net_income_2024_millions": FY2024 actual net income in USD millions (number),
  "net_income_2025e_millions": FY2025 estimated net income in USD millions (number),
  "net_income_2026e_millions": FY2026 estimated net income in USD millions (number),
  "revenue_2023_millions": FY2023 actual revenue in USD millions (for computing 2024 growth)
}}

CRITICAL: Return ONLY the JSON array, nothing else. No markdown, no code blocks, no explanations."""

    response = perplexity_query(user_prompt, api_key, system_prompt=system_prompt)
    if response is None:
        return None

    # Debug: show raw response preview
    print(f"  [DEBUG] Raw response ({len(response)} chars): {response[:400]}...")
    data = extract_json_from_response(response)
    if data is None:
        print(f"  [WARN] Failed to parse JSON from response. First 300 chars:")
        print(f"  {response[:300]}")
        return None

    return data


# âââ Data Processing ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def parse_numeric(val):
    """Parse a value that might be a number or formatted string."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val) if val != 0 else None
    if isinstance(val, str):
        s = val.strip().replace(",", "").replace("$", "")
        s = s.replace(" ", "")
        if not s or s.lower() in ("null", "n/a", "na", "nm", "none", "-", ""):
            return None
        multiplier = 1.0
        if s.upper().endswith("B"):
            multiplier = 1000.0
            s = s[:-1]
        elif s.upper().endswith("M"):
            multiplier = 1.0
            s = s[:-1]
        elif s.upper().endswith("K"):
            multiplier = 0.001
            s = s[:-1]
        try:
            return float(s) * multiplier if float(s) != 0 else None
        except ValueError:
            return None
    return None

def safe_div(a, b):
    """Safe division returning None if not possible."""
    if a is None or b is None or b == 0:
        return None
    return a / b


def process_company(raw_data, company_meta):
    """Transform Perplexity response data into dashboard format."""
    result = {
        "ticker": company_meta["display_ticker"],
        "fmp_ticker": company_meta["ticker"],
        "name": company_meta["name"],
        "category": company_meta["category"],
    }

    # Current price metrics
    result["share_price"] = parse_numeric(raw_data.get("share_price"))
    result["market_cap"] = parse_numeric(raw_data.get("market_cap_millions"))
    result["enterprise_value"] = parse_numeric(raw_data.get("enterprise_value_millions"))

    # 52-week metrics
    high = parse_numeric(raw_data.get("high_52wk"))
    low = parse_numeric(raw_data.get("low_52wk"))
    price = result.get("share_price")
    result["year_high"] = high
    result["year_low"] = low
    if price and high and high > 0:
        result["pct_52wk"] = round((price / high) * 100, 1)

    # Revenue
    rev_2023 = parse_numeric(raw_data.get("revenue_2023_millions"))
    rev_2024 = parse_numeric(raw_data.get("revenue_2024_millions"))
    rev_2025e = parse_numeric(raw_data.get("revenue_2025e_millions"))
    rev_2026e = parse_numeric(raw_data.get("revenue_2026e_millions"))

    result["revenue_2024"] = rev_2024
    result["revenue_2025e"] = rev_2025e
    result["revenue_2026e"] = rev_2026e

    # Revenue growth
    result["rev_growth_2024"] = safe_div(rev_2024 - rev_2023, rev_2023) if rev_2024 and rev_2023 else None
    result["rev_growth_2025"] = safe_div(rev_2025e - rev_2024, rev_2024) if rev_2025e and rev_2024 else None

    # EBITDA
    ebitda_2024 = parse_numeric(raw_data.get("ebitda_2024_millions"))
    ebitda_2025e = parse_numeric(raw_data.get("ebitda_2025e_millions"))
    ebitda_2026e = parse_numeric(raw_data.get("ebitda_2026e_millions"))

    result["ebitda_2024"] = ebitda_2024
    result["ebitda_2025e"] = ebitda_2025e
    result["ebitda_2026e"] = ebitda_2026e

    # EBITDA margins
    result["ebitda_margin_2024"] = safe_div(ebitda_2024, rev_2024)
    result["ebitda_margin_2025"] = safe_div(ebitda_2025e, rev_2025e)
    result["ebitda_margin_2026"] = safe_div(ebitda_2026e, rev_2026e)

    # Net income
    ni_2024 = parse_numeric(raw_data.get("net_income_2024_millions"))
    ni_2025e = parse_numeric(raw_data.get("net_income_2025e_millions"))
    ni_2026e = parse_numeric(raw_data.get("net_income_2026e_millions"))

    # Trading multiples
    ev = result.get("enterprise_value")
    mc = result.get("market_cap")

    if ev:
        result["ev_rev_2024"] = safe_div(ev, rev_2024)
        result["ev_rev_2025"] = safe_div(ev, rev_2025e)
        result["ev_rev_2026"] = safe_div(ev, rev_2026e)
        result["ev_ebitda_2024"] = safe_div(ev, ebitda_2024)
        result["ev_ebitda_2025"] = safe_div(ev, ebitda_2025e)
        result["ev_ebitda_2026"] = safe_div(ev, ebitda_2026e)

    if mc:
        result["pe_2024"] = safe_div(mc, ni_2024) if ni_2024 and ni_2024 > 0 else None
        result["pe_2025"] = safe_div(mc, ni_2025e) if ni_2025e and ni_2025e > 0 else None
        result["pe_2026"] = safe_div(mc, ni_2026e) if ni_2026e and ni_2026e > 0 else None

    return result


def match_response_to_company(raw_item, batch):
    """Match a Perplexity response item to a company in the batch."""
    ticker_id = raw_item.get("ticker_id", "")

    # Direct match on ticker
    for c in batch:
        if c["ticker"] == ticker_id:
            return c

    # Fallback: match on display ticker
    for c in batch:
        if c["display_ticker"] == ticker_id:
            return c

    # Fallback: partial match
    for c in batch:
        if ticker_id in c["ticker"] or c["ticker"] in ticker_id:
            return c
        if ticker_id in c["name"] or c["display_ticker"] in ticker_id:
            return c

    return None


def validate_company(data):
    """Validate company data quality. Returns (is_valid, issues)."""
    issues = []

    if not data.get("share_price"):
        issues.append("no share_price")
    if not data.get("market_cap"):
        issues.append("no market_cap")
    if not data.get("enterprise_value"):
        issues.append("no enterprise_value")

    has_revenue = any([
        data.get("revenue_2024"),
        data.get("revenue_2025e"),
        data.get("revenue_2026e"),
    ])
    if not has_revenue:
        issues.append("no revenue data")

    # Sanity checks on margins
    for suffix in ["_2024", "_2025", "_2026"]:
        margin = data.get(f"ebitda_margin{suffix}")
        if margin is not None and (margin < -1.0 or margin > 0.8):
            issues.append(f"ebitda_margin{suffix} out of range: {margin:.2f}")

    return len(issues) == 0, issues


def compute_category_averages(companies_data, categories):
    """Compute mean and median for each category."""
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
            values = [c[m] for c in cat_companies if c.get(m) is not None and isinstance(c.get(m), (int, float)) and c[m] > 0]
            if values:
                cat_summary["mean"][m] = round(statistics.mean(values), 4)
                cat_summary["median"][m] = round(statistics.median(values), 4)

        summaries[cat] = cat_summary

    # Overall
    all_vals = {}
    for m in metrics:
        values = [c[m] for c in companies_data if c.get(m) is not None and isinstance(c.get(m), (int, float)) and c[m] > 0]
        if values:
            all_vals[m] = {
                "mean": round(statistics.mean(values), 4),
                "median": round(statistics.median(values), 4),
            }
    summaries["Overall"] = all_vals

    return summaries


# âââ Main âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def main():
    parser = argparse.ArgumentParser(description="CPMA Public Comps Data Feed (Perplexity)")
    parser.add_argument("--api-key", default=None, help="Perplexity API key (default: env var)")
    parser.add_argument("--output", default=None, help="Output JSON path")
    parser.add_argument("--batch-size", type=int, default=5, help="Companies per API call")
    args = parser.parse_args()

    api_key = args.api_key or os.environ.get("PERPLEXITY_API_KEY", "")
    if not api_key:
        print("ERROR: No Perplexity API key. Set PERPLEXITY_API_KEY env var or use --api-key.")
        sys.exit(1)

    output_path = args.output or str(Path(__file__).parent / "cpma_comps_data.json")
    batch_size = args.batch_size

    print("=" * 60)
    print("CPMA Public Comps Data Feed (Perplexity Sonar API)")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Companies: {len(COMPANIES)}")
    print(f"Batch size: {batch_size}")
    print("=" * 60)

    # ââ Fetch company data in batches ââ
    companies_data = []
    failed_companies = []
    total_batches = (len(COMPANIES) + batch_size - 1) // batch_size

    for i in range(0, len(COMPANIES), batch_size):
        batch = COMPANIES[i:i + batch_size]
        batch_num = i // batch_size + 1
        tickers = ", ".join(c["display_ticker"] for c in batch)
        print(f"\n[Batch {batch_num}/{total_batches}] {tickers}")

        raw_data = fetch_batch(batch, api_key)

        if raw_data is None:
            print(f"  [WARN] Batch failed. Retrying companies individually...")
            for company in batch:
                print(f"  Retrying {company['display_ticker']}...")
                individual = fetch_batch([company], api_key)
                if individual:
                    raw_data = (raw_data or []) + individual
                else:
                    failed_companies.append(company)
                time.sleep(2)

        if raw_data:
            matched_tickers = set()
            for raw_item in raw_data:
                company_meta = match_response_to_company(raw_item, batch)
                if company_meta is None:
                    print(f"  [WARN] Could not match response item: {raw_item.get('ticker_id', 'unknown')}")
                    continue

                matched_tickers.add(company_meta["ticker"])
                processed = process_company(raw_item, company_meta)
                valid, issues = validate_company(processed)

                if not valid:
                    print(f"  [WARN] {company_meta['display_ticker']}: {', '.join(issues)}")

                companies_data.append(processed)

            # Check for companies not in response
            for c in batch:
                if c["ticker"] not in matched_tickers:
                    print(f"  [WARN] No data returned for {c['display_ticker']}")
                    # Add stub entry
                    companies_data.append({
                        "ticker": c["display_ticker"],
                        "fmp_ticker": c["ticker"],
                        "name": c["name"],
                        "category": c["category"],
                    })
        else:
            # Complete batch failure
            for c in batch:
                print(f"  [ERROR] No data for {c['display_ticker']}")
                companies_data.append({
                    "ticker": c["display_ticker"],
                    "fmp_ticker": c["ticker"],
                    "name": c["name"],
                    "category": c["category"],
                })

        # Rate limiting between batches
        if batch_num < total_batches:
            time.sleep(2)

    # ââ Data quality gate ââ
    has_price = sum(1 for c in companies_data if c.get("share_price"))
    has_ev = sum(1 for c in companies_data if c.get("enterprise_value"))
    has_revenue = sum(1 for c in companies_data if c.get("revenue_2024"))

    print(f"\n{'=' * 60}")
    print("Data Quality Report:")
    print(f"  Companies with price:     {has_price}/{len(companies_data)}")
    print(f"  Companies with EV:        {has_ev}/{len(companies_data)}")
    print(f"  Companies with revenue:   {has_revenue}/{len(companies_data)}")

    # Abort if data quality is too low
    MIN_PRICE_THRESHOLD = 30
    MIN_REVENUE_THRESHOLD = 25

    if has_price < MIN_PRICE_THRESHOLD:
        print(f"\n[ABORT] Only {has_price} companies have price data (need {MIN_PRICE_THRESHOLD}+).")
        print("Aborting to prevent dashboard corruption.")
        sys.exit(1)

    if has_revenue < MIN_REVENUE_THRESHOLD:
        print(f"\n[ABORT] Only {has_revenue} companies have revenue data (need {MIN_REVENUE_THRESHOLD}+).")
        print("Aborting to prevent dashboard corruption.")
        sys.exit(1)

    # ââ Compute category summaries ââ
    print("\nComputing category summaries...")
    summaries = compute_category_averages(companies_data, CATEGORIES)

    # ââ Build output ââ
    output = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "source": "Perplexity Sonar API (Fiscal.ai/Morningstar)",
            "num_companies": len(companies_data),
            "categories": CATEGORIES,
        },
        "companies": companies_data,
        "category_summaries": summaries,
    }

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\nData saved to: {output_path}")
    print(f"Companies processed: {len(companies_data)}")
    print("=" * 60)


if __name__ == "__main__":
    main()

if __name__ == "__main__":
        main()
