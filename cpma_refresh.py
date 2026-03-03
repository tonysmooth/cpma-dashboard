#!/usr/bin/env python3
"""
CPMA Dashboard Refresh Script
==============================
One-command refresh: fetches live market data from FMP API and rebuilds
the dashboard HTML with the new embedded data.

Usage:
    python cpma_refresh.py --api-key YOUR_FMP_KEY
    python cpma_refresh.py --api-key YOUR_FMP_KEY --dashboard-dir /path/to/files

Requirements:
    - Python 3.8+
    - FMP API key (get free at https://financialmodelingprep.com/developer/docs/)
    - cpma_data_feed.py in the same directory (or specify with --feed-script)
    - cpma_dashboard.html in the same directory (or specify with --dashboard-dir)

What it does:
    1. Runs cpma_data_feed.py to fetch fresh data from FMP API
    2. Reads the generated cpma_comps_data.json
    3. Thins daily price history to weekly points (keeps file size manageable)
    4. Replaces the embedded DATA block in cpma_dashboard.html
    5. Updates the "Last Updated" timestamp in the dashboard header
"""

import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def thin_price_history(data):
    """Reduce daily price points to weekly to keep HTML file size reasonable."""
    thinned = {}

    # Thin individual company price history
    if "price_history" in data:
        for ticker, series in data["price_history"].items():
            if isinstance(series, list) and len(series) > 60:
                # Keep every 5th point (roughly weekly) plus the last point
                thinned_series = series[::5]
                if series[-1] != thinned_series[-1]:
                    thinned_series.append(series[-1])
                thinned[ticker] = thinned_series
            else:
                thinned[ticker] = series
        data["price_history"] = thinned

    # Thin category price series
    if "category_price_series" in data:
        thinned_cat = {}
        for cat, series in data["category_price_series"].items():
            if isinstance(series, list) and len(series) > 60:
                thinned_series = series[::5]
                if series[-1] != thinned_series[-1]:
                    thinned_series.append(series[-1])
                thinned_cat[cat] = thinned_series
            else:
                thinned_cat[cat] = series
        data["category_price_series"] = thinned_cat

    return data


def rebuild_dashboard(dashboard_path, data_json):
    """Replace the embedded DATA block in the dashboard HTML."""
    with open(dashboard_path, "r", encoding="utf-8") as f:
        html = f.read()

    # Find and replace the DATA block: let DATA = {...};
    # The pattern matches from "let DATA = " to the next ";\n\n//"
    pattern = r'let DATA = \{.*?\};\s*\n\s*\n\s*//'

    # Use a simpler, more robust approach: find the start and end markers
    start_marker = "let DATA = "
    start_idx = html.find(start_marker)
    if start_idx == -1:
        print("ERROR: Could not find 'let DATA = ' in dashboard HTML")
        return False

    # Find the end of the JSON object by counting braces
    json_start = start_idx + len(start_marker)
    brace_count = 0
    end_idx = json_start
    in_string = False
    escape_next = False

    for i in range(json_start, len(html)):
        ch = html[i]
        if escape_next:
            escape_next = False
            continue
        if ch == '\\' and in_string:
            escape_next = True
            continue
        if ch == '"' and not escape_next:
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == '{':
            brace_count += 1
        elif ch == '}':
            brace_count -= 1
            if brace_count == 0:
                end_idx = i + 1
                break

    if brace_count != 0:
        print("ERROR: Could not find matching closing brace for DATA object")
        return False

    # Also skip the semicolon after the JSON
    if end_idx < len(html) and html[end_idx] == ';':
        end_idx += 1

    # Build new DATA assignment
    compact_json = json.dumps(data_json, separators=(',', ':'), default=str)
    new_data_block = f"let DATA = {compact_json};"

    # Replace
    new_html = html[:start_idx] + new_data_block + html[end_idx:]    if end_idx < len(html) and html[end_idx] == ';':
        end_idx += 1

    # Build new DATA assignment
    compact_json = json.dumps(data_json, separators=(',', ':'), default=str)
    new_data_block = f"let DATA = {compact_json};"

    # Replace
    new_html = html[:start_idx] + new_data_block + html[end_idx:]

    # Update the "Last Updated" text in the header
    today_str = datetime.now().strftime("%B %d, %Y")
    # Look for patterns like "As of February 27, 2026" or similar date strings
    new_html = re.sub(
        r'As of \w+ \d{1,2}, \d{4}',
        f'As of {today_str}',
        new_html
    )
    # Also update any "Data as of" or "Updated" timestamps
    new_html = re.sub(
        r'Data as of \w+ \d{1,2}, \d{4}',
        f'Data as of {today_str}',
        new_html
    )

    with open(dashboard_path, "w", encoding="utf-8") as f:
        f.write(new_html)

    return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CPMA Dashboard Refresh")
    parser.add_argument("--api-key", required=True, help="FMP API key")
    parser.add_argument("--dashboard-dir", default=None,
                        help="Directory containing dashboard files (default: same as this script)")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="Skip API fetch, just rebuild HTML from existing JSON")
    args = parser.parse_args()

    # Determine file paths
    base_dir = Path(args.dashboard_dir) if args.dashboard_dir else Path(__file__).parent
    feed_script = base_dir / "cpma_data_feed.py"
    json_path = base_dir / "cpma_comps_data.json"
    # Support both filenames: index.html (GitHub Pages) or cpma_dashboard.html (local)
    dashboard_path = base_dir / "index.html"
    if not dashboard_path.exists():
        dashboard_path = base_dir / "cpma_dashboard.html"

    print("=" * 60)
    print("CPMA Dashboard Refresh")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Directory: {base_dir}")
    print("=" * 60)

    # Step 1: Fetch fresh data
    if not args.skip_fetch:
        print("\n[Step 1/3] Fetching fresh market data from FMP API...")
        if not feed_script.exists():
            print(f"ERROR: Data feed script not found at {feed_script}")
            sys.exit(1)

        result = subprocess.run(
            [sys.executable, str(feed_script), "--api-key", args.api_key, "--output", str(json_path)],
            capture_output=True, text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            print(f"ERROR: Data feed script failed:\n{result.stderr}")
            sys.exit(1)
    else:
        print("\n[Step 1/3] Skipping API fetch (using existing JSON)...")

    # Step 2: Load and thin the data
    print("\n[Step 2/3] Processing data (thinning price history)...")
    if not json_path.exists():
        print(f"ERROR: JSON data not found at {json_path}")
        sys.exit(1)

    with open(json_path, "r") as f:
        data = json.load(f)

    original_size = json_path.stat().st_size
    data = thin_price_history(data)
    thinned_size = len(json.dumps(data, separators=(',', ':'), default=str))
    print(f"  Full JSON: {original_size:,} bytes")
    print(f"  Thinned for embed: {thinned_size:,} bytes")

    # Step 3: Rebuild dashboard
    print("\n[Step 3/3] Rebuilding dashboard HTML with fresh data...")
    if not dashboard_path.exists():
        print(f"ERROR: Dashboard not found at {dashboard_path}")
        sys.exit(1)

    old_size = dashboard_path.stat().st_size
    success = rebuild_dashboard(dashboard_path, data)

    if success:
        new_size = dashboard_path.stat().st_size
        print(f"  Dashboard updated: {old_size:,} -> {new_size:,} bytes")
        print(f"\n{'=' * 60}")
        print(f"SUCCESS! Dashboard refreshed at {datetime.now().strftime('%H:%M:%S')}")
        print(f"  Companies: {len(data.get('companies', []))}")
        print(f"  Price series: {len(data.get('price_history', {}))}")
        print(f"  File: {dashboard_path}")
        print("=" * 60)
    else:
        print("FAILED to rebuild dashboard. See errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
