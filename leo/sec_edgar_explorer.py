"""
SEC EDGAR Data Explorer + Polygon Price Overlay Toolkit
========================================================
Author: Built for Jeff Biggs / HWP Group
Purpose: Explore SEC filing data and overlay with Polygon market data
         to identify potential alert/insight product opportunities.

This toolkit uses ONLY free SEC EDGAR APIs (no API key needed) and 
your existing Polygon.io subscription.

Usage:
    1. Set your Polygon API key below (or as environment variable POLYGON_API_KEY)
    2. Run the script: python sec_edgar_explorer.py
    3. Explore the output to see what data is available

SEC EDGAR APIs Used (all free, no auth required):
    - Submissions API: https://data.sec.gov/submissions/
    - XBRL Company Facts API: https://data.sec.gov/api/xbrl/companyfacts/
    - XBRL Frames API: https://data.sec.gov/api/xbrl/frames/
    - Full-Text Search: https://efts.sec.gov/LATEST/search-index

Rate Limit: 10 requests/second to SEC. Include User-Agent header.
"""

import requests
import json
import time
import os
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET

try:
    import pymysql
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False
    print("Note: pymysql not installed. Run 'pip install pymysql' to enable MySQL export.")

# =============================================================================
# CONFIGURATION
# =============================================================================

# Set your Polygon API key here or as environment variable
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "YOUR_POLYGON_API_KEY_HERE")

# SEC requires a User-Agent header identifying you
SEC_USER_AGENT = "HWP Group jeff@jeffreybiggs.com"

# MySQL Database Configuration
MYSQL_CONFIG = {
    'host': '127.0.0.1',
    'user': 'remote',
    'password': 'Chamba4347!',
    'database': 'leo',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor if MYSQL_AVAILABLE else None
}

# Headers for SEC requests
SEC_HEADERS = {
    "User-Agent": SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
}

# Rate limiting helper
def sec_request(url, params=None):
    """Make a rate-limited request to SEC APIs."""
    time.sleep(0.15)  # Stay well under 10 req/sec limit
    resp = requests.get(url, headers=SEC_HEADERS, params=params)
    resp.raise_for_status()
    return resp

def polygon_request(url, params=None):
    """Make a request to Polygon.io API."""
    if params is None:
        params = {}
    params["apiKey"] = POLYGON_API_KEY
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp


# =============================================================================
# PART 1: COMPANY LOOKUP & SUBMISSIONS (Filing History)
# =============================================================================

def get_company_submissions(ticker_or_cik):
    """
    Get a company's filing history from SEC EDGAR.
    
    Returns company info + recent filings (up to 1000).
    This is the foundation - it tells you every filing a company has made.
    
    Example: get_company_submissions("AAPL") or get_company_submissions("320193")
    """
    # First, resolve ticker to CIK if needed
    cik = resolve_ticker_to_cik(ticker_or_cik)
    if cik is None:
        print(f"Could not resolve '{ticker_or_cik}' to a CIK number.")
        return None
    
    # Pad CIK to 10 digits
    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    
    resp = sec_request(url)
    data = resp.json()
    
    print(f"\n{'='*70}")
    print(f"COMPANY: {data.get('name', 'Unknown')}")
    print(f"CIK: {data.get('cik', 'Unknown')}")
    print(f"Tickers: {data.get('tickers', [])}")
    print(f"SIC: {data.get('sic', '')} - {data.get('sicDescription', '')}")
    print(f"State: {data.get('stateOfIncorporation', '')}")
    print(f"Fiscal Year End: {data.get('fiscalYearEnd', '')}")
    print(f"{'='*70}")
    
    # Show recent filing counts by type
    filings = data.get("filings", {}).get("recent", {})
    forms = filings.get("form", [])
    
    if forms:
        from collections import Counter
        form_counts = Counter(forms)
        print(f"\nRecent Filing Counts (last ~1000 filings):")
        for form_type, count in form_counts.most_common(15):
            print(f"  {form_type:20s} : {count}")
    
    return data


def resolve_ticker_to_cik(ticker_or_cik):
    """Resolve a ticker symbol to CIK number using SEC's company tickers file."""
    # If it's already a number, assume it's a CIK
    if str(ticker_or_cik).isdigit():
        return int(ticker_or_cik)
    
    ticker = ticker_or_cik.upper()
    url = "https://www.sec.gov/files/company_tickers.json"
    resp = sec_request(url)
    tickers = resp.json()
    
    for entry in tickers.values():
        if entry.get("ticker", "").upper() == ticker:
            return entry["cik_str"]
    
    return None


# =============================================================================
# PART 2: INSIDER TRADING (Form 4) - Parse actual Form 4 XML
# =============================================================================

def get_insider_trades(ticker_or_cik, max_filings=20, start_date=None, end_date=None):
    """
    Get insider trades (Form 4 filings) for a company.
    
    This fetches the filing index, then parses the actual Form 4 XML
    to extract: who traded, what they traded, buy/sell, shares, price.
    
    Args:
        ticker_or_cik: Company ticker or CIK number
        max_filings: Maximum number of Form 4 filings to process
        start_date: Optional start date (YYYY-MM-DD) to filter filings
        end_date: Optional end date (YYYY-MM-DD) to filter filings
    
    THIS IS THE GOLDMINE for your alert product.
    """
    from datetime import datetime as dt
    
    cik = resolve_ticker_to_cik(ticker_or_cik)
    if cik is None:
        print(f"Could not resolve '{ticker_or_cik}'")
        return []
    
    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    resp = sec_request(url)
    data = resp.json()
    
    company_name = data.get("name", "Unknown")
    
    # Get recent filings
    filings_recent = data.get("filings", {}).get("recent", {})
    forms = list(filings_recent.get("form", []))
    accessions = list(filings_recent.get("accessionNumber", []))
    dates = list(filings_recent.get("filingDate", []))
    
    # Check if there are older filings available
    older_files = data.get("filings", {}).get("files", [])
    
    # If we need more data, fetch older filing archives
    if older_files and (max_filings > len([f for f in forms if f == "4"]) or start_date):
        print(f"  Fetching older filings... (found {len(older_files)} archive(s))")
        archives_fetched = 0
        max_archives = 20  # Safety limit to prevent excessive API calls
        
        for file_info in older_files:
            if archives_fetched >= max_archives:
                print(f"  Reached archive limit ({max_archives}), stopping fetch")
                break
                
            file_name = file_info.get("name")
            file_url = f"https://data.sec.gov/submissions/{file_name}"
            
            try:
                older_resp = sec_request(file_url)
                older_data = older_resp.json()
                
                # Append older filings to our lists
                forms.extend(older_data.get("form", []))
                accessions.extend(older_data.get("accessionNumber", []))
                dates.extend(older_data.get("filingDate", []))
                archives_fetched += 1
                
                # If we have a start_date and the oldest date in this batch is before start_date,
                # we can stop fetching more archives
                if start_date:
                    oldest_dates = older_data.get("filingDate", [])
                    if oldest_dates and min(oldest_dates) < start_date:
                        print(f"  Reached filings before start date, stopping archive fetch")
                        break
                
            except Exception as e:
                print(f"  Warning: Could not fetch {file_name}: {e}")
                break
    
    # Filter for Form 4 filings with optional date range
    form4_indices = []
    for i, f in enumerate(forms):
        if f == "4":
            filing_date = dates[i]
            
            # Apply date filters if provided
            if start_date and filing_date < start_date:
                continue
            if end_date and filing_date > end_date:
                continue
            
            form4_indices.append(i)
            
            # Stop if we've reached max_filings (but only if no start_date is specified)
            # When start_date is provided, get ALL filings in the date range
            if not start_date and len(form4_indices) >= max_filings:
                break
    
    print(f"\n{'='*70}")
    print(f"INSIDER TRADES (Form 4) - {company_name}")
    if start_date or end_date:
        date_range = f" from {start_date or 'beginning'} to {end_date or 'present'}"
        print(f"Found {len(form4_indices)} Form 4 filings{date_range}")
        if len(form4_indices) > 0:
            actual_dates = [dates[i] for i in form4_indices]
            print(f"Date range of filings: {min(actual_dates)} to {max(actual_dates)}")
    else:
        print(f"Found {len(form4_indices)} Form 4 filings (showing up to {max_filings})")
    print(f"{'='*70}")
    
    trades = []
    
    for idx in form4_indices:
        accession_no_dash = accessions[idx].replace("-", "")
        accession_dashed = accessions[idx]
        filing_date = dates[idx]
        
        # Get the .txt file which contains the raw XML
        txt_url = (
            f"https://www.sec.gov/Archives/edgar/data/"
            f"{cik}/{accession_no_dash}/{accession_dashed}.txt"
        )
        
        try:
            txt_resp = sec_request(txt_url)
            txt_content = txt_resp.text
            
            # Extract XML from between <XML> and </XML> tags
            import re
            xml_match = re.search(r'<XML>(.*?)</XML>', txt_content, re.DOTALL)
            if xml_match:
                xml_content = xml_match.group(1).strip()
                trade_info = parse_form4_xml(xml_content, filing_date)
                if trade_info and trade_info.get("transactions"):
                    trades.append(trade_info)
                    print_trade(trade_info)
                # Silently skip filings with no transactions
            else:
                print(f"  [{filing_date}] No XML found in .txt file")
                
        except Exception as e:
            print(f"  [{filing_date}] Error fetching/parsing: {e}")
    
    return trades


def parse_form4_xml(xml_text, filing_date):
    """Parse a Form 4 XML document to extract trade details."""
    try:
        # Strip namespace declarations to make parsing easier
        # Common SEC namespaces
        namespaces_to_remove = [
            'xmlns="http://www.sec.gov/edgar/document/ownership/xml/0001"',
            'xmlns="http://www.sec.gov/edgar/ownershipdocument"',
            'xmlns="http://www.sec.gov/cgi-bin/viewer?action=view&cik='
        ]
        for ns in namespaces_to_remove:
            xml_text = xml_text.replace(ns, '')
        
        # Also remove xmlns= declarations more generically
        import re
        xml_text = re.sub(r'xmlns="[^"]*"', '', xml_text)
        
        root = ET.fromstring(xml_text)
        
        # Extract reporter (insider) info
        reporter_name = ""
        reporter_title = ""
        is_director = False
        is_officer = False
        is_ten_percent_owner = False
        
        # Look for reporting owner - try multiple methods
        for owner in root.iter():
            tag = owner.tag.split("}")[-1] if "}" in owner.tag else owner.tag
            
            # Name fields
            if tag == "rptOwnerName":
                reporter_name = owner.text or ""
            elif tag == "rptOwnerCik" and not reporter_name:
                # Fallback to CIK if no name found
                pass
            
            # Title and roles
            if tag == "officerTitle" and owner.text:
                reporter_title = owner.text
            if tag == "isDirector" and owner.text:
                is_director = owner.text.strip() == "1"
            if tag == "isOfficer" and owner.text:
                is_officer = owner.text.strip() == "1"
            if tag == "isTenPercentOwner" and owner.text:
                is_ten_percent_owner = owner.text.strip() == "1"
        
        # Extract non-derivative transactions (actual stock transactions)
        transactions = []
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            
            if tag == "nonDerivativeTransaction":
                txn = {}
                
                # Iterate through this transaction's children
                for child in elem.iter():
                    child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    
                    if child_tag == "transactionDate":
                        # Look for value child
                        for v in child:
                            v_tag = v.tag.split("}")[-1] if "}" in v.tag else v.tag
                            if v_tag == "value" and v.text:
                                txn["date"] = v.text
                    
                    elif child_tag == "transactionCoding":
                        # Transaction code is nested in transactionCoding
                        for code_elem in child:
                            code_tag = code_elem.tag.split("}")[-1] if "}" in code_elem.tag else code_elem.tag
                            if code_tag == "transactionCode" and code_elem.text:
                                txn["code"] = code_elem.text
                    
                    elif child_tag == "transactionAmounts":
                        # Shares and price are in transactionAmounts
                        for amt_elem in child:
                            amt_tag = amt_elem.tag.split("}")[-1] if "}" in amt_elem.tag else amt_elem.tag
                            
                            if amt_tag == "transactionShares":
                                for v in amt_elem:
                                    v_tag = v.tag.split("}")[-1] if "}" in v.tag else v.tag
                                    if v_tag == "value" and v.text:
                                        try:
                                            txn["shares"] = float(v.text)
                                        except (ValueError, TypeError):
                                            pass
                            
                            elif amt_tag == "transactionPricePerShare":
                                for v in amt_elem:
                                    v_tag = v.tag.split("}")[-1] if "}" in v.tag else v.tag
                                    if v_tag == "value" and v.text:
                                        try:
                                            txn["price"] = float(v.text)
                                        except (ValueError, TypeError):
                                            pass
                            
                            elif amt_tag == "transactionAcquiredDisposedCode":
                                for v in amt_elem:
                                    v_tag = v.tag.split("}")[-1] if "}" in v.tag else v.tag
                                    if v_tag == "value" and v.text:
                                        txn["acquired_disposed"] = v.text
                    
                    elif child_tag == "postTransactionAmounts":
                        # Shares owned after transaction
                        for post_elem in child:
                            post_tag = post_elem.tag.split("}")[-1] if "}" in post_elem.tag else post_elem.tag
                            if post_tag == "sharesOwnedFollowingTransaction":
                                for v in post_elem:
                                    v_tag = v.tag.split("}")[-1] if "}" in v.tag else v.tag
                                    if v_tag == "value" and v.text:
                                        try:
                                            txn["shares_after"] = float(v.text)
                                        except (ValueError, TypeError):
                                            pass
                
                # Only add if we got meaningful data
                if txn and ("shares" in txn or "code" in txn):
                    transactions.append(txn)
        
        # Build role string
        roles = []
        if is_officer and reporter_title:
            roles.append(reporter_title)
        elif is_officer:
            roles.append("Officer")
        if is_director:
            roles.append("Director")
        if is_ten_percent_owner:
            roles.append("10%+ Owner")
        
        # Only return if we have reporter name
        if not reporter_name:
            return None
        
        return {
            "filing_date": filing_date,
            "reporter": reporter_name,
            "role": ", ".join(roles) if roles else "Reporting Person",
            "transactions": transactions,
        }
        
    except Exception as e:
        # Return None on any parsing error
        return None


def print_trade(trade_info):
    """Pretty-print a parsed insider trade."""
    print(f"\n  Filing Date: {trade_info['filing_date']}")
    print(f"  Insider:     {trade_info['reporter']}")
    print(f"  Role:        {trade_info['role']}")
    
    for txn in trade_info.get("transactions", []):
        code = txn.get("code", "?")
        code_map = {
            "P": "PURCHASE (Open Market)",
            "S": "SALE (Open Market)", 
            "A": "GRANT/AWARD",
            "M": "OPTION EXERCISE",
            "F": "TAX WITHHOLDING",
            "G": "GIFT",
            "C": "CONVERSION",
            "D": "DISPOSITION TO ISSUER",
        }
        code_desc = code_map.get(code, f"Code: {code}")
        
        shares = txn.get("shares", 0)
        price = txn.get("price", 0)
        total_value = shares * price if price else 0
        direction = txn.get("acquired_disposed", "?")
        
        print(f"  Transaction: {code_desc}")
        print(f"    Date:      {txn.get('date', 'N/A')}")
        print(f"    Shares:    {shares:,.0f}")
        if price:
            print(f"    Price:     ${price:,.2f}")
            print(f"    Value:     ${total_value:,.2f}")
        if txn.get("shares_after"):
            print(f"    Holds After: {txn['shares_after']:,.0f} shares")
    print(f"  {'-'*50}")


# =============================================================================
# PART 3: FORM 8-K (Material Events)
# =============================================================================

def get_8k_filings(ticker_or_cik, max_filings=10, start_date=None, end_date=None):
    """
    Get recent 8-K filings (material events) for a company.
    
    8-K items tell you WHAT happened:
      1.01 = Entry into Material Agreement
      1.02 = Termination of Material Agreement
      2.01 = Completion of Acquisition/Disposition
      2.02 = Results of Operations (Earnings)
      2.05 = Costs for Exit/Disposal
      2.06 = Material Impairments
      3.01 = Delisting
      4.01 = Change in Accountant
      4.02 = Non-Reliance on Financial Statements (RESTATEMENT!)
      5.01 = Change in Control
      5.02 = Departure/Election of Directors/Officers
      5.03 = Amendments to Articles
      7.01 = Regulation FD Disclosure
      8.01 = Other Events
      9.01 = Financial Statements and Exhibits
    """
    cik = resolve_ticker_to_cik(ticker_or_cik)
    if cik is None:
        print(f"Could not resolve '{ticker_or_cik}'")
        return []
    
    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    resp = sec_request(url)
    data = resp.json()
    
    company_name = data.get("name", "Unknown")
    
    # Get recent filings
    filings_recent = data.get("filings", {}).get("recent", {})
    forms = list(filings_recent.get("form", []))
    accessions = list(filings_recent.get("accessionNumber", []))
    dates = list(filings_recent.get("filingDate", []))
    items_list = list(filings_recent.get("items", []))
    
    # Check if there are older filings available
    older_files = data.get("filings", {}).get("files", [])
    
    # If we need more data, fetch older filing archives
    if older_files and (max_filings > len([f for f in forms if f == "8-K"]) or start_date):
        print(f"  Fetching older filings... (found {len(older_files)} archive(s))")
        for file_info in older_files:
            file_name = file_info.get("name")
            file_url = f"https://data.sec.gov/submissions/{file_name}"
            
            try:
                older_resp = sec_request(file_url)
                older_data = older_resp.json()
                
                # Append older filings to our lists
                forms.extend(older_data.get("form", []))
                accessions.extend(older_data.get("accessionNumber", []))
                dates.extend(older_data.get("filingDate", []))
                items_list.extend(older_data.get("items", []))
                
            except Exception as e:
                print(f"  Warning: Could not fetch {file_name}: {e}")
                break
    
    # Filter for 8-K filings with optional date range
    eightk_indices = []
    for i, f in enumerate(forms):
        if f == "8-K":
            filing_date = dates[i]
            
            # Apply date filters if provided
            if start_date and filing_date < start_date:
                continue
            if end_date and filing_date > end_date:
                continue
            
            eightk_indices.append(i)
            
            # Stop if we've reached max_filings
            if len(eightk_indices) >= max_filings:
                break
    
    # Map of 8-K item codes to human-readable descriptions
    item_descriptions = {
        "1.01": "Entry into Material Agreement",
        "1.02": "Termination of Material Agreement",
        "1.03": "Bankruptcy or Receivership",
        "2.01": "Completion of Acquisition or Disposition",
        "2.02": "Results of Operations / Financial Condition",
        "2.03": "Creation of Direct Financial Obligation",
        "2.04": "Triggering Events (Acceleration of Obligations)",
        "2.05": "Exit/Disposal Costs",
        "2.06": "Material Impairments",
        "3.01": "Delisting or Transfer of Listing",
        "3.02": "Unregistered Sales of Equity",
        "3.03": "Material Modification of Rights",
        "4.01": "Change in Accountant",
        "4.02": "Non-Reliance on Financial Statements *** RESTATEMENT ***",
        "5.01": "Change in Control of Registrant",
        "5.02": "Departure/Election of Directors or Officers",
        "5.03": "Amendments to Articles/Bylaws",
        "5.07": "Shareholder Vote Submission",
        "7.01": "Regulation FD Disclosure",
        "8.01": "Other Events",
        "9.01": "Financial Statements and Exhibits",
    }
    
    print(f"\n{'='*70}")
    print(f"8-K MATERIAL EVENT FILINGS - {company_name}")
    if start_date or end_date:
        date_range = f" from {start_date or 'beginning'} to {end_date or 'present'}"
        print(f"Found {len(eightk_indices)} 8-K filings{date_range}")
    else:
        print(f"Found {len(eightk_indices)} 8-K filings")
    print(f"{'='*70}")
    
    results = []
    
    for idx in eightk_indices:
        filing_date = dates[idx]
        items_raw = items_list[idx] if idx < len(items_list) else ""
        accession = accessions[idx]
        
        # Parse item codes
        item_codes = [i.strip() for i in items_raw.split(",") if i.strip()]
        item_descs = []
        for code in item_codes:
            desc = item_descriptions.get(code, f"Item {code}")
            item_descs.append(f"  {code}: {desc}")
        
        # Flag high-impact items
        high_impact_items = {"1.01", "1.02", "2.01", "2.02", "2.06", "4.02", 
                            "5.01", "5.02"}
        is_high_impact = bool(set(item_codes) & high_impact_items)
        flag = " ** HIGH IMPACT **" if is_high_impact else ""
        
        result = {
            "filing_date": filing_date,
            "accession": accession,
            "items": item_codes,
            "item_descriptions": item_descs,
            "high_impact": is_high_impact,
        }
        results.append(result)
        
        print(f"\n  Date: {filing_date}{flag}")
        print(f"  Accession: {accession}")
        if item_descs:
            print(f"  Items:")
            for d in item_descs:
                print(f"    {d}")
        else:
            print(f"  Items: (none listed)")
        print(f"  {'-'*50}")
    
    return results


# =============================================================================
# PART 4: XBRL FINANCIAL DATA
# =============================================================================

def get_financial_facts(ticker_or_cik, concepts=None):
    """
    Get XBRL financial data for a company.
    
    Concepts are US-GAAP tags like:
      - Revenues
      - NetIncomeLoss
      - EarningsPerShareBasic
      - Assets
      - Liabilities
      - StockholdersEquity
      - OperatingIncomeLoss
      - CashAndCashEquivalentsAtCarryingValue
    
    Returns historical quarterly/annual values for each concept.
    """
    if concepts is None:
        concepts = [
            "Revenues",
            "NetIncomeLoss", 
            "EarningsPerShareBasic",
            "Assets",
            "StockholdersEquity",
        ]
    
    cik = resolve_ticker_to_cik(ticker_or_cik)
    if cik is None:
        print(f"Could not resolve '{ticker_or_cik}'")
        return {}
    
    cik_padded = str(cik).zfill(10)
    
    # Get all company facts
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
    resp = sec_request(url)
    data = resp.json()
    
    entity_name = data.get("entityName", "Unknown")
    us_gaap = data.get("facts", {}).get("us-gaap", {})
    
    print(f"\n{'='*70}")
    print(f"FINANCIAL DATA (XBRL) - {entity_name}")
    print(f"{'='*70}")
    
    # Show what concepts are available
    available = sorted(us_gaap.keys())
    print(f"\nTotal US-GAAP concepts available: {len(available)}")
    print(f"(Full list saved to company_concepts.txt if you want to browse)")
    
    results = {}
    
    for concept in concepts:
        if concept not in us_gaap:
            print(f"\n  {concept}: NOT AVAILABLE for this company")
            # Try to find close matches
            matches = [c for c in available if concept.lower() in c.lower()][:5]
            if matches:
                print(f"    Similar concepts: {', '.join(matches)}")
            continue
        
        concept_data = us_gaap[concept]
        label = concept_data.get("label", concept)
        units = concept_data.get("units", {})
        
        print(f"\n  {label} ({concept}):")
        
        # Get the primary unit (usually USD or USD/shares)
        for unit_name, facts in units.items():
            # Show last 8 entries
            recent = facts[-8:]
            print(f"    Unit: {unit_name}")
            print(f"    Recent values:")
            for fact in recent:
                period_end = fact.get("end", fact.get("instant", "N/A"))
                val = fact.get("val", 0)
                form = fact.get("form", "?")
                filed = fact.get("filed", "?")
                
                # Format large numbers
                if abs(val) >= 1_000_000_000:
                    val_str = f"${val/1_000_000_000:,.2f}B"
                elif abs(val) >= 1_000_000:
                    val_str = f"${val/1_000_000:,.2f}M"
                elif abs(val) >= 1000:
                    val_str = f"${val:,.0f}"
                else:
                    val_str = f"{val:,.4f}"
                
                print(f"      {period_end} | {val_str:>15s} | Filed: {filed} ({form})")
            
            results[concept] = facts
    
    return results


# =============================================================================
# PART 5: FULL-TEXT SEARCH (Search across all filings)
# =============================================================================

def search_filings(query, date_range=None, form_types=None, max_results=10):
    """
    Full-text search across all EDGAR filings since 2001.
    
    Great for finding specific events, keywords, or patterns.
    
    Examples:
        search_filings("substantial doubt going concern")
        search_filings("cybersecurity breach", form_types=["8-K"])
        search_filings("restatement material weakness", form_types=["8-K", "10-K"])
    """
    url = "https://efts.sec.gov/LATEST/search-index"
    
    params = {
        "q": query,
        "dateRange": date_range or "custom",
        "startdt": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
        "enddt": datetime.now().strftime("%Y-%m-%d"),
    }
    
    if form_types:
        params["forms"] = ",".join(form_types)
    
    # Use the EDGAR full text search endpoint
    search_url = "https://efts.sec.gov/LATEST/search-index"
    params = {
        "q": query,
        "dateRange": "custom",
        "startdt": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
        "enddt": datetime.now().strftime("%Y-%m-%d"),
    }
    if form_types:
        params["forms"] = ",".join(form_types)
    
    resp = sec_request(search_url, params=params)
    
    # The EFTS API returns JSON
    try:
        data = resp.json()
    except Exception:
        # Fallback: try the main search endpoint
        search_url = "https://efts.sec.gov/LATEST/search-index"
        params["_source"] = "file_date,period_of_report,form_type,entity_name,file_num"
        resp = sec_request(search_url, params=params)
        data = resp.json()
    
    hits = data.get("hits", {}).get("hits", [])
    total = data.get("hits", {}).get("total", {}).get("value", 0)
    
    print(f"\n{'='*70}")
    print(f"FULL-TEXT SEARCH: '{query}'")
    print(f"Total results: {total}")
    print(f"{'='*70}")
    
    results = []
    for hit in hits[:max_results]:
        source = hit.get("_source", {})
        print(f"\n  Company:  {source.get('entity_name', 'N/A')}")
        print(f"  Form:     {source.get('form_type', 'N/A')}")
        print(f"  Filed:    {source.get('file_date', 'N/A')}")
        print(f"  Period:   {source.get('period_of_report', 'N/A')}")
        results.append(source)
    
    return results


# =============================================================================
# PART 6: POLYGON PRICE DATA OVERLAY
# =============================================================================

def get_polygon_price_around_date(ticker, event_date, days_before=5, days_after=20):
    """
    Get price data from Polygon around a specific SEC filing date.
    
    This is where the magic happens - overlay market data with filing events.
    
    Returns daily bars for the window around the event.
    
    NOTE: Requires your Polygon API key to be set above.
    """
    if POLYGON_API_KEY == "YOUR_POLYGON_API_KEY_HERE":
        print("\n  WARNING: Set your POLYGON_API_KEY to use price overlay features.")
        print("  Edit the script and replace YOUR_POLYGON_API_KEY_HERE")
        return None
    
    event_dt = datetime.strptime(event_date, "%Y-%m-%d")
    start_dt = event_dt - timedelta(days=days_before + 5)  # Extra buffer for weekends
    end_dt = event_dt + timedelta(days=days_after + 5)
    
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_dt.strftime('%Y-%m-%d')}/{end_dt.strftime('%Y-%m-%d')}"
    
    try:
        resp = polygon_request(url, {"adjusted": "true", "sort": "asc"})
        data = resp.json()
        
        bars = data.get("results", [])
        if not bars:
            print(f"  No price data found for {ticker} around {event_date}")
            return None
        
        # Find the bar closest to the event date
        event_ts = event_dt.timestamp() * 1000
        
        print(f"\n  Price Data for {ticker} around {event_date}:")
        print(f"  {'Date':12s} {'Open':>10s} {'High':>10s} {'Low':>10s} {'Close':>10s} {'Volume':>15s}")
        print(f"  {'-'*67}")
        
        event_close = None
        pre_event_close = None
        post_bars = []
        
        for bar in bars:
            bar_date = datetime.fromtimestamp(bar["t"] / 1000).strftime("%Y-%m-%d")
            bar_dt = datetime.strptime(bar_date, "%Y-%m-%d")
            
            marker = ""
            if bar_date == event_date:
                marker = " <-- FILING DATE"
                event_close = bar["c"]
            
            # Track pre-event close
            if bar_dt < event_dt:
                pre_event_close = bar["c"]
            
            # Track post-event bars
            if bar_dt > event_dt:
                post_bars.append(bar)
            
            print(f"  {bar_date:12s} {bar['o']:10.2f} {bar['h']:10.2f} {bar['l']:10.2f} {bar['c']:10.2f} {bar['v']:15,.0f}{marker}")
        
        # Calculate impact metrics
        if pre_event_close and event_close:
            day_change = ((event_close - pre_event_close) / pre_event_close) * 100
            print(f"\n  Filing Day Change: {day_change:+.2f}%")
        
        if event_close and post_bars:
            # 5-day and 20-day post-event returns
            for target_days, label in [(5, "5-Day"), (20, "20-Day")]:
                if len(post_bars) >= target_days:
                    post_close = post_bars[target_days - 1]["c"]
                    post_return = ((post_close - event_close) / event_close) * 100
                    print(f"  {label} Post-Filing Return: {post_return:+.2f}%")
        
        return bars
        
    except Exception as e:
        print(f"  Error fetching Polygon data: {e}")
        return None


def get_polygon_options_activity(ticker, event_date, days_before=5):
    """
    Check options activity around a filing date using Polygon.
    
    Unusual options volume before an SEC filing could indicate
    informed trading - a powerful alert signal.
    
    NOTE: Requires Polygon Options subscription.
    """
    if POLYGON_API_KEY == "YOUR_POLYGON_API_KEY_HERE":
        print("\n  WARNING: Set your POLYGON_API_KEY to use options data.")
        return None
    
    event_dt = datetime.strptime(event_date, "%Y-%m-%d")
    start_dt = event_dt - timedelta(days=days_before + 3)
    
    # Get options aggregates (total volume per day)
    # This uses the Polygon options snapshot endpoint
    url = f"https://api.polygon.io/v3/snapshot/options/{ticker}"
    
    try:
        resp = polygon_request(url)
        data = resp.json()
        
        results = data.get("results", [])
        if not results:
            print(f"  No options snapshot data for {ticker}")
            return None
        
        total_call_vol = 0
        total_put_vol = 0
        
        for opt in results:
            details = opt.get("details", {})
            day = opt.get("day", {})
            
            if details.get("contract_type") == "call":
                total_call_vol += day.get("volume", 0)
            elif details.get("contract_type") == "put":
                total_put_vol += day.get("volume", 0)
        
        total = total_call_vol + total_put_vol
        ratio = total_call_vol / total_put_vol if total_put_vol > 0 else float("inf")
        
        print(f"\n  Options Activity Snapshot for {ticker}:")
        print(f"    Total Call Volume:  {total_call_vol:,}")
        print(f"    Total Put Volume:   {total_put_vol:,}")
        print(f"    Call/Put Ratio:     {ratio:.2f}")
        
        return {
            "call_volume": total_call_vol,
            "put_volume": total_put_vol,
            "ratio": ratio,
        }
        
    except Exception as e:
        print(f"  Error fetching options data: {e}")
        return None


# =============================================================================
# PART 7: COMBINED ANALYSIS - THE PRODUCT PROTOTYPE
# =============================================================================

def analyze_insider_trades_with_prices(ticker, max_trades=10):
    """
    THE KEY FUNCTION: Combines insider trades with price data.
    
    This is the core of your potential alert product.
    For each insider trade, it shows:
      1. What the insider did (buy/sell/grant)
      2. The price context (was stock going up or down?)
      3. What happened to the stock AFTER the filing
    
    Run this on a few tickers to see the patterns emerge.
    """
    print(f"\n{'#'*70}")
    print(f"# INSIDER TRADE + PRICE ANALYSIS: {ticker}")
    print(f"# This is your product prototype - enriched insider alerts")
    print(f"{'#'*70}")
    
    # Get insider trades
    trades = get_insider_trades(ticker, max_filings=max_trades)
    
    if not trades:
        print("No insider trades found.")
        return
    
    # For each trade with a purchase or sale, overlay price data
    for trade in trades:
        filing_date = trade["filing_date"]
        
        # Check if there are actual buy/sell transactions (not just grants)
        interesting_txns = [
            t for t in trade.get("transactions", [])
            if t.get("code") in ("P", "S")  # Purchases and Sales
        ]
        
        if interesting_txns:
            print(f"\n  >>> Open Market {'BUY' if interesting_txns[0].get('code') == 'P' else 'SELL'} detected")
            print(f"  >>> Insider: {trade['reporter']} ({trade['role']})")
            
            for txn in interesting_txns:
                shares = txn.get("shares", 0)
                price = txn.get("price", 0)
                total = shares * price if price else 0
                print(f"  >>> {shares:,.0f} shares @ ${price:,.2f} = ${total:,.2f}")
            
            # Now overlay with price data
            get_polygon_price_around_date(ticker, filing_date)
            print()


def analyze_8k_with_prices(ticker, max_filings=5):
    """
    Combine 8-K material events with price data.
    
    Shows how the market reacted to material events.
    Great for identifying which types of 8-K events move stocks.
    """
    print(f"\n{'#'*70}")
    print(f"# 8-K MATERIAL EVENT + PRICE ANALYSIS: {ticker}")
    print(f"{'#'*70}")
    
    filings = get_8k_filings(ticker, max_filings=max_filings)
    
    for filing in filings:
        if filing.get("high_impact"):
            print(f"\n  >>> HIGH IMPACT 8-K on {filing['filing_date']}")
            for desc in filing.get("item_descriptions", []):
                print(f"  >>> {desc}")
            
            get_polygon_price_around_date(ticker, filing["filing_date"])
            print()


# =============================================================================
# PART 8: MYSQL DATABASE HELPERS
# =============================================================================

def get_mysql_connection():
    """Get a MySQL database connection."""
    if not MYSQL_AVAILABLE:
        raise ImportError("pymysql is not installed. Run: pip install pymysql")
    
    return pymysql.connect(**MYSQL_CONFIG)


def create_mysql_tables():
    """Create MySQL tables for storing SEC data."""
    if not MYSQL_AVAILABLE:
        print("ERROR: pymysql not installed. Run: pip install pymysql")
        return False
    
    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    try:
        # Table 1: Insider Trades
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS insider_trades (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL,
                filing_date DATE NOT NULL,
                insider_name VARCHAR(255),
                insider_role VARCHAR(255),
                transaction_code VARCHAR(10),
                transaction_date DATE,
                shares DECIMAL(20, 4),
                price_per_share DECIMAL(20, 4),
                total_value DECIMAL(20, 4),
                acquired_disposed VARCHAR(1),
                shares_owned_after DECIMAL(20, 4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_trade (ticker, filing_date, insider_name, transaction_date, shares, transaction_code),
                INDEX idx_ticker (ticker),
                INDEX idx_filing_date (filing_date),
                INDEX idx_transaction_date (transaction_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        
        # Table 2: 8-K Filings
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS filings_8k (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL,
                company_name VARCHAR(255),
                filing_date DATE NOT NULL,
                accession_number VARCHAR(50) NOT NULL,
                item_codes TEXT,
                high_impact BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_filing (ticker, accession_number),
                INDEX idx_ticker (ticker),
                INDEX idx_filing_date (filing_date),
                INDEX idx_high_impact (high_impact)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        
        # Table 3: Financial Facts
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS financial_facts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL,
                company_name VARCHAR(255),
                concept VARCHAR(255) NOT NULL,
                concept_label VARCHAR(255),
                unit VARCHAR(50),
                period_end DATE,
                value DECIMAL(30, 4),
                form VARCHAR(20),
                filed_date DATE,
                fiscal_year VARCHAR(10),
                fiscal_period VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_fact (ticker, concept, period_end, form, filed_date),
                INDEX idx_ticker (ticker),
                INDEX idx_concept (concept),
                INDEX idx_period_end (period_end)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        
        conn.commit()
        print("SUCCESS: MySQL tables created/verified successfully")
        return True
        
    except Exception as e:
        print(f"ERROR creating MySQL tables: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def save_insider_trades_mysql(ticker, trades):
    """Save insider trades to MySQL database."""
    if not MYSQL_AVAILABLE:
        print("ERROR: pymysql not installed")
        return 0
    
    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    inserted = 0
    duplicates = 0
    
    try:
        for trade in trades:
            for txn in trade.get("transactions", []):
                shares = txn.get("shares", 0)
                price = txn.get("price", 0)
                total_value = shares * price if price else None
                
                try:
                    cursor.execute("""
                        INSERT INTO insider_trades 
                        (ticker, filing_date, insider_name, insider_role, transaction_code,
                         transaction_date, shares, price_per_share, total_value,
                         acquired_disposed, shares_owned_after)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE id=id
                    """, (
                        ticker,
                        trade["filing_date"],
                        trade["reporter"],
                        trade["role"],
                        txn.get("code", ""),
                        txn.get("date"),
                        shares,
                        price if price else None,
                        total_value,
                        txn.get("acquired_disposed", ""),
                        txn.get("shares_after")
                    ))
                    
                    if cursor.rowcount > 0:
                        inserted += 1
                    else:
                        duplicates += 1
                        
                except Exception as e:
                    print(f"  Warning: Error inserting trade: {e}")
                    continue
        
        conn.commit()
        print(f"  SUCCESS: Inserted {inserted} trades, skipped {duplicates} duplicates")
        return inserted
        
    except Exception as e:
        print(f"  ERROR saving to MySQL: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()


def save_8k_filings_mysql(ticker, company_name, filings_data):
    """Save 8-K filings to MySQL database."""
    if not MYSQL_AVAILABLE:
        print("ERROR: pymysql not installed")
        return 0
    
    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    inserted = 0
    duplicates = 0
    
    try:
        for filing in filings_data:
            try:
                cursor.execute("""
                    INSERT INTO filings_8k 
                    (ticker, company_name, filing_date, accession_number, item_codes, high_impact)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE id=id
                """, (
                    ticker,
                    company_name,
                    filing["filing_date"],
                    filing["accession"],
                    "; ".join(filing.get("items", [])),
                    filing.get("high_impact", False)
                ))
                
                if cursor.rowcount > 0:
                    inserted += 1
                else:
                    duplicates += 1
                    
            except Exception as e:
                print(f"  Warning: Error inserting 8-K: {e}")
                continue
        
        conn.commit()
        print(f"  SUCCESS: Inserted {inserted} 8-K filings, skipped {duplicates} duplicates")
        return inserted
        
    except Exception as e:
        print(f"  ERROR saving to MySQL: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()


def save_financial_facts_mysql(ticker, company_name, facts_data):
    """Save financial facts to MySQL database."""
    if not MYSQL_AVAILABLE:
        print("ERROR: pymysql not installed")
        return 0
    
    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    inserted = 0
    duplicates = 0
    
    try:
        for fact in facts_data:
            try:
                cursor.execute("""
                    INSERT INTO financial_facts 
                    (ticker, company_name, concept, concept_label, unit, period_end, 
                     value, form, filed_date, fiscal_year, fiscal_period)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE value=VALUES(value)
                """, (
                    ticker,
                    company_name,
                    fact["concept"],
                    fact["label"],
                    fact["unit"],
                    fact["period_end"],
                    fact["value"],
                    fact["form"],
                    fact["filed_date"],
                    fact["fiscal_year"],
                    fact["fiscal_period"]
                ))
                
                if cursor.rowcount > 0:
                    inserted += 1
                else:
                    duplicates += 1
                    
            except Exception as e:
                print(f"  Warning: Error inserting financial fact: {e}")
                continue
        
        conn.commit()
        print(f"  SUCCESS: Inserted {inserted} financial facts, skipped {duplicates} duplicates")
        return inserted
        
    except Exception as e:
        print(f"  ERROR saving to MySQL: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()


# =============================================================================
# PART 9: DATA EXPORT HELPERS (CSV)
# =============================================================================

def get_date_range(months_back=None, years_back=None):
    """
    Helper function to generate date range strings for filtering filings by date.
    
    This lets you go back in time to get older filings instead of just
    the most recent ones.
    
    Args:
        months_back: Number of months back from today (approximate, 30 days/month)
        years_back: Number of years back from today (365 days/year)
    
    Returns:
        tuple: (start_date, end_date) in YYYY-MM-DD format
    
    Examples:
        # Get last 3 months
        start, end = get_date_range(months_back=3)
        export_insider_trades_csv("AAPL", start_date=start, end_date=end, max_filings=200)
        
        # Get last 6 months
        start, end = get_date_range(months_back=6)
        export_all_data_csv("TSLA", start_date=start, end_date=end, max_filings=300)
        
        # Get last year
        start, end = get_date_range(years_back=1)
        export_insider_trades_csv("NVDA", start_date=start, max_filings=500)
        
        # Get last 2 years
        start, end = get_date_range(years_back=2)
        export_8k_filings_csv("META", start_date=start, max_filings=1000)
    """
    from datetime import datetime, timedelta
    
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    if months_back:
        # Approximate: 30 days per month
        days_back = months_back * 30
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    elif years_back:
        days_back = years_back * 365
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    else:
        start_date = None
    
    return start_date, end_date

def export_insider_trades_csv(ticker, output_file=None, max_filings=50, start_date=None, end_date=None):
    """
    Export insider trades to CSV for analysis in Excel or pandas.
    
    Args:
        ticker: Company ticker symbol
        output_file: Optional output filename
        max_filings: Maximum number of filings to export
        start_date: Optional start date (YYYY-MM-DD) to filter filings
        end_date: Optional end date (YYYY-MM-DD) to filter filings
    
    This gives you a flat file you can easily work with.
    """
    import csv
    
    if output_file is None:
        output_file = f"{ticker}_insider_trades.csv"
    
    trades = get_insider_trades(ticker, max_filings=max_filings, start_date=start_date, end_date=end_date)
    
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "filing_date", "insider_name", "role", 
            "transaction_code", "transaction_date",
            "shares", "price_per_share", "total_value",
            "acquired_or_disposed", "shares_owned_after"
        ])
        
        for trade in trades:
            for txn in trade.get("transactions", []):
                shares = txn.get("shares", 0)
                price = txn.get("price", 0)
                writer.writerow([
                    trade["filing_date"],
                    trade["reporter"],
                    trade["role"],
                    txn.get("code", ""),
                    txn.get("date", ""),
                    shares,
                    price,
                    shares * price if price else "",
                    txn.get("acquired_disposed", ""),
                    txn.get("shares_after", ""),
                ])
    
    print(f"\n  SUCCESS: Exported {len(trades)} insider trades to: {output_file}")
    return output_file


def export_8k_filings_csv(ticker, output_file=None, max_filings=50, start_date=None, end_date=None):
    """
    Export 8-K material event filings to CSV.
    
    Args:
        ticker: Company ticker symbol
        output_file: Optional output filename
        max_filings: Maximum number of filings to export
        start_date: Optional start date (YYYY-MM-DD) to filter filings
        end_date: Optional end date (YYYY-MM-DD) to filter filings
    """
    import csv
    
    if output_file is None:
        output_file = f"{ticker}_8k_filings.csv"
    
    # Use the get_8k_filings function which now supports date filtering
    filings = get_8k_filings(ticker, max_filings=max_filings, start_date=start_date, end_date=end_date)
    
    # Get company name
    cik = resolve_ticker_to_cik(ticker)
    if cik is None:
        print(f"Could not resolve '{ticker}'")
        return None
    
    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    resp = sec_request(url)
    data = resp.json()
    company_name = data.get("name", "Unknown")
    
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "company_name", "ticker", "filing_date", "accession_number", 
            "item_codes", "high_impact"
        ])
        
        for filing in filings:
            writer.writerow([
                company_name,
                ticker,
                filing["filing_date"],
                filing["accession"],
                "; ".join(filing["items"]),
                "YES" if filing["high_impact"] else "NO"
            ])
    
    print(f"\n  SUCCESS: Exported {len(filings)} 8-K filings to: {output_file}")
    return output_file


def export_financial_facts_csv(ticker, output_file=None, concepts=None):
    """
    Export XBRL financial facts to CSV.
    """
    import csv
    
    if output_file is None:
        output_file = f"{ticker}_financial_facts.csv"
    
    if concepts is None:
        concepts = [
            "Revenues",
            "NetIncomeLoss", 
            "EarningsPerShareBasic",
            "Assets",
            "StockholdersEquity",
            "OperatingIncomeLoss",
            "CashAndCashEquivalentsAtCarryingValue",
        ]
    
    cik = resolve_ticker_to_cik(ticker)
    if cik is None:
        print(f"Could not resolve '{ticker}'")
        return None
    
    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
    resp = sec_request(url)
    data = resp.json()
    
    entity_name = data.get("entityName", "Unknown")
    us_gaap = data.get("facts", {}).get("us-gaap", {})
    
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "company_name", "ticker", "concept", "label", "unit",
            "period_end", "value", "form", "filed_date", "fiscal_year", "fiscal_period"
        ])
        
        for concept in concepts:
            if concept not in us_gaap:
                continue
            
            concept_data = us_gaap[concept]
            label = concept_data.get("label", concept)
            units = concept_data.get("units", {})
            
            for unit_name, facts in units.items():
                for fact in facts:
                    period_end = fact.get("end", fact.get("instant", ""))
                    val = fact.get("val", "")
                    form = fact.get("form", "")
                    filed = fact.get("filed", "")
                    fiscal_year = fact.get("fy", "")
                    fiscal_period = fact.get("fp", "")
                    
                    writer.writerow([
                        entity_name,
                        ticker,
                        concept,
                        label,
                        unit_name,
                        period_end,
                        val,
                        form,
                        filed,
                        fiscal_year,
                        fiscal_period
                    ])
    
    print(f"\n  SUCCESS: Exported financial facts to: {output_file}")
    return output_file


def export_all_data_mysql(ticker, max_filings=50, start_date=None, end_date=None):
    """
    Export all available data for a ticker to MySQL database.
    
    Args:
        ticker: Company ticker symbol
        max_filings: Maximum number of filings to export
        start_date: Optional start date (YYYY-MM-DD) to filter filings
        end_date: Optional end date (YYYY-MM-DD) to filter filings
    
    Saves to MySQL tables:
      - insider_trades
      - filings_8k
      - financial_facts
    """
    if not MYSQL_AVAILABLE:
        print("\nERROR: pymysql is not installed.")
        print("Install it with: pip install pymysql")
        return False
    
    print(f"\n{'='*70}")
    print(f"EXPORTING ALL DATA TO MYSQL FOR: {ticker}")
    if start_date or end_date:
        print(f"Date range: {start_date or 'beginning'} to {end_date or 'present'}")
    print(f"{'='*70}")
    
    # Ensure tables exist
    if not create_mysql_tables():
        return False
    
    # Get company info
    cik = resolve_ticker_to_cik(ticker)
    if cik is None:
        print(f"Could not resolve '{ticker}'")
        return False
    
    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    resp = sec_request(url)
    data = resp.json()
    company_name = data.get("name", "Unknown")
    
    total_inserted = 0
    
    # Export Insider Trades
    print(f"\n>>> Exporting Insider Trades to MySQL...")
    try:
        trades = get_insider_trades(ticker, max_filings=max_filings, 
                                   start_date=start_date, end_date=end_date)
        inserted = save_insider_trades_mysql(ticker, trades)
        total_inserted += inserted
    except Exception as e:
        print(f"  WARNING: Error exporting insider trades: {e}")
    
    # Export 8-K Filings
    print(f"\n>>> Exporting 8-K Filings to MySQL...")
    try:
        # Get 8-K data
        filings_recent = data.get("filings", {}).get("recent", {})
        forms = list(filings_recent.get("form", []))
        accessions = list(filings_recent.get("accessionNumber", []))
        dates = list(filings_recent.get("filingDate", []))
        items_list = list(filings_recent.get("items", []))
        
        # Filter for 8-K filings with date range
        eightk_data = []
        for i, f in enumerate(forms):
            if f == "8-K":
                filing_date = dates[i]
                
                if start_date and filing_date < start_date:
                    continue
                if end_date and filing_date > end_date:
                    continue
                
                items_raw = items_list[i] if i < len(items_list) else ""
                item_codes = [it.strip() for it in items_raw.split(",") if it.strip()]
                
                high_impact_items = {"1.01", "1.02", "2.01", "2.02", "2.06", "4.02", "5.01", "5.02"}
                is_high_impact = bool(set(item_codes) & high_impact_items)
                
                eightk_data.append({
                    "filing_date": filing_date,
                    "accession": accessions[i],
                    "items": item_codes,
                    "high_impact": is_high_impact
                })
                
                if len(eightk_data) >= max_filings:
                    break
        
        inserted = save_8k_filings_mysql(ticker, company_name, eightk_data)
        total_inserted += inserted
    except Exception as e:
        print(f"  WARNING: Error exporting 8-K filings: {e}")
    
    # Export Financial Facts
    print(f"\n>>> Exporting Financial Facts to MySQL...")
    try:
        concepts = [
            "Revenues", "NetIncomeLoss", "EarningsPerShareBasic",
            "Assets", "StockholdersEquity", "OperatingIncomeLoss",
            "CashAndCashEquivalentsAtCarryingValue"
        ]
        
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
        resp = sec_request(url)
        facts_data_raw = resp.json()
        
        entity_name = facts_data_raw.get("entityName", "Unknown")
        us_gaap = facts_data_raw.get("facts", {}).get("us-gaap", {})
        
        facts_to_save = []
        for concept in concepts:
            if concept not in us_gaap:
                continue
            
            concept_data = us_gaap[concept]
            label = concept_data.get("label", concept)
            units = concept_data.get("units", {})
            
            for unit_name, facts in units.items():
                for fact in facts:
                    period_end = fact.get("end", fact.get("instant", ""))
                    val = fact.get("val", "")
                    form = fact.get("form", "")
                    filed = fact.get("filed", "")
                    fiscal_year = fact.get("fy", "")
                    fiscal_period = fact.get("fp", "")
                    
                    facts_to_save.append({
                        "concept": concept,
                        "label": label,
                        "unit": unit_name,
                        "period_end": period_end,
                        "value": val,
                        "form": form,
                        "filed_date": filed,
                        "fiscal_year": fiscal_year,
                        "fiscal_period": fiscal_period
                    })
        
        inserted = save_financial_facts_mysql(ticker, entity_name, facts_to_save)
        total_inserted += inserted
    except Exception as e:
        print(f"  WARNING: Error exporting financial facts: {e}")
    
    print(f"\n{'='*70}")
    print(f"MYSQL EXPORT COMPLETE!")
    print(f"Total records inserted/updated: {total_inserted}")
    print(f"{'='*70}")
    
    return True


def export_all_data_csv(ticker, max_filings=50, start_date=None, end_date=None):
    """
    Export all available data for a ticker to separate CSV files.
    
    Args:
        ticker: Company ticker symbol
        max_filings: Maximum number of filings to export
        start_date: Optional start date (YYYY-MM-DD) to filter filings
        end_date: Optional end date (YYYY-MM-DD) to filter filings
    
    Creates:
      - {ticker}_insider_trades.csv
      - {ticker}_8k_filings.csv
      - {ticker}_financial_facts.csv
    """
    print(f"\n{'='*70}")
    print(f"EXPORTING ALL DATA FOR: {ticker}")
    if start_date or end_date:
        print(f"Date range: {start_date or 'beginning'} to {end_date or 'present'}")
    print(f"{'='*70}")
    
    files_created = []
    
    try:
        file = export_insider_trades_csv(ticker, max_filings=max_filings, 
                                         start_date=start_date, end_date=end_date)
        if file:
            files_created.append(file)
    except Exception as e:
        print(f"  WARNING: Error exporting insider trades: {e}")
    
    try:
        file = export_8k_filings_csv(ticker, max_filings=max_filings,
                                     start_date=start_date, end_date=end_date)
        if file:
            files_created.append(file)
    except Exception as e:
        print(f"  WARNING: Error exporting 8-K filings: {e}")
    
    try:
        file = export_financial_facts_csv(ticker)
        if file:
            files_created.append(file)
    except Exception as e:
        print(f"  WARNING: Error exporting financial facts: {e}")
    
    print(f"\n{'='*70}")
    print(f"EXPORT COMPLETE!")
    print(f"Files created: {len(files_created)}")
    for f in files_created:
        print(f"  - {f}")
    print(f"{'='*70}")
    
    return files_created


# =============================================================================
# MAIN - Run the exploration
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='SEC EDGAR Data Explorer - Download insider trades, 8-K filings, and financial data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export all data for NVDA to CSV (default: last ~50 filings)
  python sec_edgar_explorer.py
  
  # Export last 6 months of AAPL data to CSV
  python sec_edgar_explorer.py --ticker AAPL --months 6
  
  # Export last 12 months of TSLA data to MySQL
  python sec_edgar_explorer.py -t TSLA -m 12 --max-filings 500 --mysql
  
  # Export last 2 years of META data to MySQL
  python sec_edgar_explorer.py -t META --years 2 --max-filings 1000 --mysql
  
  # Export specific date range to MySQL
  python sec_edgar_explorer.py -t NVDA --start-date 2024-01-01 --end-date 2024-12-31 --mysql
  
  # Export only (skip preview display)
  python sec_edgar_explorer.py -t AAPL -m 6 --export-only --mysql
        """
    )
    
    parser.add_argument('-t', '--ticker', 
                        default='NVDA',
                        help='Stock ticker symbol (default: NVDA)')
    
    parser.add_argument('-m', '--months',
                        type=int,
                        help='Number of months back to fetch (e.g., 6 for last 6 months)')
    
    parser.add_argument('-y', '--years',
                        type=int,
                        help='Number of years back to fetch (e.g., 2 for last 2 years)')
    
    parser.add_argument('--start-date',
                        help='Start date in YYYY-MM-DD format (e.g., 2024-01-01)')
    
    parser.add_argument('--end-date',
                        help='End date in YYYY-MM-DD format (e.g., 2024-12-31)')
    
    parser.add_argument('--max-filings',
                        type=int,
                        default=50,
                        help='Maximum number of filings to process (default: 50)')
    
    parser.add_argument('--export-only',
                        action='store_true',
                        help='Skip preview display and only export data')
    
    parser.add_argument('--mysql',
                        action='store_true',
                        help='Export to MySQL database instead of CSV files')
    
    args = parser.parse_args()
    
    # Determine date range
    start_date = args.start_date
    end_date = args.end_date
    
    if args.months:
        start_date, end_date = get_date_range(months_back=args.months)
    elif args.years:
        start_date, end_date = get_date_range(years_back=args.years)
    
    print("""
    ====================================================================
    =         SEC EDGAR Data Explorer + Polygon Overlay              =
    =         Built for HWP Group - Product Exploration              =
    ====================================================================
    
    This script explores what SEC data is available and shows how
    it can be combined with Polygon market data for alert generation.
    """)
    
    TICKER = args.ticker
    
    print(f"\n{'='*70}")
    print(f"  TICKER: {TICKER}")
    if start_date:
        print(f"  DATE RANGE: {start_date} to {end_date or 'present'}")
    print(f"  MAX FILINGS: {args.max_filings}")
    print(f"{'='*70}")
    
    if not args.export_only:
        # -- STEP 1: Preview company info and filings --
        print("\n>>> Getting company information...")
        submissions = get_company_submissions(TICKER)
        
        # -- STEP 2: Preview recent insider trades --
        print("\n>>> Getting recent insider trades (preview)...")
        trades = get_insider_trades(TICKER, max_filings=5, start_date=start_date, end_date=end_date)
        
        # -- STEP 3: Preview recent 8-K material events --
        print("\n>>> Getting recent 8-K filings (preview)...")
        events = get_8k_filings(TICKER, max_filings=5)
        
        # -- STEP 4: Preview financial data --
        print("\n>>> Getting financial data (preview)...")
        financials = get_financial_facts(TICKER)
    
    # -- STEP 5: Export all data --
    if args.mysql:
        print(f"\n{'='*70}")
        print(">>> EXPORTING ALL DATA TO MYSQL...")
        print(f"{'='*70}")
        
        export_all_data_mysql(
            TICKER, 
            max_filings=args.max_filings,
            start_date=start_date,
            end_date=end_date
        )
    else:
        print(f"\n{'='*70}")
        print(">>> EXPORTING ALL DATA TO CSV...")
        print(f"{'='*70}")
        
        export_all_data_csv(
            TICKER, 
            max_filings=args.max_filings,
            start_date=start_date,
            end_date=end_date
        )
    
    print(f"\n\n{'='*70}")
    print("  NEXT STEPS & EXAMPLES:")
    print(f"{'='*70}")
    print("\n  1. Check the CSV files created in this directory")
    print("\n  2. Try different time ranges:")
    print("     python sec_edgar_explorer.py -t AAPL --months 6")
    print("     python sec_edgar_explorer.py -t TSLA --months 12 --max-filings 500")
    print("     python sec_edgar_explorer.py -t META --years 2 --max-filings 1000")
    print("\n  3. Use custom date ranges:")
    print("     python sec_edgar_explorer.py -t NVDA --start-date 2024-01-01 --end-date 2024-12-31")
    print("\n  4. Export only (skip preview):")
    print("     python sec_edgar_explorer.py -t AAPL -m 6 --export-only")
    print("\n  5. See all options:")
    print("     python sec_edgar_explorer.py --help")
    print("\n  6. Set your POLYGON_API_KEY to enable price overlay features")
    print("\n  7. Analyze the CSV data in Excel/Pandas/your favorite tool")
    print(f"\n{'='*70}")
