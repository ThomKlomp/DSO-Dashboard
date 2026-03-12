"""
generate_dashboard.py
=====================
1. Fetches all open/late/reminded invoices from Moneybird API
2. Writes docs/data.json  (chart data + all open invoices per customer)

Usage:
    python generate_dashboard.py              # generate only
    python generate_dashboard.py --push       # generate + git push
    python generate_dashboard.py --loop 1     # loop every 1 hour + push
"""

import argparse, datetime, json, math, os, subprocess, sys, time
import requests

MONEYBIRD_TOKEN   = os.getenv("MONEYBIRD_TOKEN",    "YOUR_TOKEN_HERE")
ADMINISTRATION_ID = os.getenv("MONEYBIRD_ADMIN_ID", "YOUR_ADMIN_ID_HERE")
DSO_TARGET        = int(os.getenv("DSO_TARGET", "38"))
OUT_DIR           = os.path.join(os.path.dirname(__file__), "docs")

BASE_URL = f"https://moneybird.com/api/v2/{ADMINISTRATION_ID}"
HEADERS  = {"Authorization": f"Bearer {MONEYBIRD_TOKEN}"}
TODAY    = datetime.date.today()


def api_get(path, params=None):
    url, results = f"{BASE_URL}/{path}", []
    while url:
        r = requests.get(url, headers=HEADERS, params=params or {})
        if r.status_code == 401: sys.exit("❌  Invalid Moneybird token.")
        if r.status_code == 404: sys.exit("❌  Invalid Administration ID.")
        r.raise_for_status()
        d = r.json()
        if isinstance(d, list): results.extend(d)
        else: return d
        url = r.links.get("next", {}).get("url"); params = None
    return results


def fetch():
    """Fetch all open/late/reminded invoices — these are ALL open invoices."""
    out = []
    for state in ["open", "late", "reminded"]:
        out.extend(api_get("sales_invoices.json", {"filter": f"state:{state}"}))
    return out


def parse(inv):
    c       = inv.get("contact") or {}
    due_str = inv.get("due_date") or inv.get("invoice_date")
    due     = datetime.date.fromisoformat(due_str) if due_str else TODAY
    days    = max(0, (TODAY - due).days)
    amount  = float(inv.get("total_unpaid_base") or inv.get("total_price_incl_tax") or 0)
    company = (c.get("company_name") or
               f"{c.get('firstname','')} {c.get('lastname','')}".strip() or "Unknown")
    contact_id = inv.get("contact_id") or (c.get("id") if c else None) or ""

    def bucket(d):
        if d == 0:   return "Current"
        if d <= 30:  return "1-30 days"
        if d <= 60:  return "31-60 days"
        if d <= 90:  return "61-90 days"
        return ">90 days"

    score = round((amount/1000)*math.log(max(amount,1)+1) + days*0.5, 1)
    level = "URGENT" if score >= 70 else ("High" if score >= 40 else "Monitor")

    return {
        "id":           inv["id"],
        "contact_id":   str(contact_id),
        "invoice_no":   inv.get("invoice_id") or inv.get("reference") or inv["id"],
        "company":      company,
        "invoice_date": inv.get("invoice_date", ""),
        "due_date":     due.isoformat(),
        "amount":       round(amount, 2),
        "currency":     inv.get("currency", "EUR"),
        "days_overdue": days,
        "bucket":       bucket(days),
        "state":        inv.get("state", ""),
        "score":        score,
        "level":        level,
    }


def build_data(invoices):
    total_ar   = sum(i["amount"] for i in invoices)
    recent_rev = sum(i["amount"] for i in invoices
                     if i["invoice_date"] and
                     (TODAY - datetime.date.fromisoformat(i["invoice_date"])).days <= 30)
    avg_daily  = recent_rev / 30 if recent_rev else 1
    dso        = round(total_ar / avg_daily, 1) if avg_daily else 0

    buckets = ["Current","1-30 days","31-60 days","61-90 days",">90 days"]
    aging   = {b: round(sum(i["amount"] for i in invoices if i["bucket"]==b), 2)
               for b in buckets}

    overdue = sorted([i for i in invoices if i["days_overdue"] > 0],
                     key=lambda x: x["score"], reverse=True)

    # Build customer index: contact_id -> { company, all open invoices }
    customers = {}
    for inv in invoices:
        cid = inv["contact_id"]
        if cid not in customers:
            customers[cid] = {
                "contact_id": cid,
                "company":    inv["company"],
                "invoices":   [],
            }
        customers[cid]["invoices"].append(inv)

    # Sort each customer's invoices: overdue first, then by amount desc
    for cid in customers:
        customers[cid]["invoices"].sort(
            key=lambda x: (-(x["days_overdue"] > 0), -x["amount"])
        )
        # Add customer-level summary
        invs = customers[cid]["invoices"]
        customers[cid]["total_open"]    = round(sum(i["amount"] for i in invs), 2)
        customers[cid]["total_overdue"] = round(sum(i["amount"] for i in invs if i["days_overdue"] > 0), 2)
        customers[cid]["max_days"]      = max((i["days_overdue"] for i in invs), default=0)
        customers[cid]["invoice_count"] = len(invs)
        scores = [i["score"] for i in invs if i["days_overdue"] > 0]
        top_score = max(scores) if scores else 0
        customers[cid]["level"] = "URGENT" if top_score >= 70 else ("High" if top_score >= 40 else "Monitor")

    # DSO history
    hist_file = os.path.join(OUT_DIR, "history.json")
    history = []
    if os.path.exists(hist_file):
        with open(hist_file) as f:
            history = json.load(f)
    today_str = TODAY.isoformat()
    history = [h for h in history if h["date"] != today_str]
    history.append({"date": today_str, "dso": dso, "target": DSO_TARGET})
    history = history[-90:]
    with open(hist_file, "w") as f:
        json.dump(history, f)

    return {
        "generated":     datetime.datetime.now().isoformat(),
        "dso":           dso,
        "dso_target":    DSO_TARGET,
        "total_ar":      round(total_ar, 2),
        "invoice_count": len(invoices),
        "overdue_30":    round(sum(i["amount"] for i in invoices if i["days_overdue"] > 30), 2),
        "overdue_60":    round(sum(i["amount"] for i in invoices if i["days_overdue"] > 60), 2),
        "overdue_90":    round(sum(i["amount"] for i in invoices if i["days_overdue"] > 90), 2),
        "aging":         aging,
        "history":       history,
        "invoices":      invoices,
        "overdue_top":   overdue[:20],
        "customers":     customers,
    }


def git_push():
    cmds = [
        ["git", "-C", OUT_DIR+"/..", "add", "docs/"],
        ["git", "-C", OUT_DIR+"/..", "commit", "-m", f"chore: DSO sync {TODAY.isoformat()}"],
        ["git", "-C", OUT_DIR+"/..", "push"],
    ]
    for cmd in cmds:
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0 and "nothing to commit" not in r.stdout:
            print(f"  git: {r.stderr.strip() or r.stdout.strip()}")
        else:
            print(f"  ✅ {' '.join(cmd[2:])}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--push", action="store_true")
    parser.add_argument("--loop", type=int, default=0, metavar="HOURS")
    args = parser.parse_args()

    if MONEYBIRD_TOKEN == "YOUR_TOKEN_HERE":
        sys.exit("❌  Set MONEYBIRD_TOKEN environment variable first.")

    def run():
        print(f"\n🔄  [{datetime.datetime.now().strftime('%H:%M')}] Fetching from Moneybird...")
        raw  = fetch()
        invs = [parse(i) for i in raw]
        data = build_data(invs)
        os.makedirs(OUT_DIR, exist_ok=True)
        with open(os.path.join(OUT_DIR, "data.json"), "w") as f:
            json.dump(data, f, indent=2)
        print(f"  ✅  data.json written ({len(invs)} invoices, {len(data['customers'])} customers, DSO: {data['dso']} days)")
        if args.push:
            git_push()
        else:
            print("  ℹ️   Run with --push to publish to GitHub Pages")

    run()
    if args.loop:
        print(f"\n⏰  Looping every {args.loop}h. Ctrl+C to stop.")
        while True:
            time.sleep(args.loop * 3600)
            run()

if __name__ == "__main__":
    main()
