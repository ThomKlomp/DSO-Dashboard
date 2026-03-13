"""
generate_dashboard.py
=====================
Fetches invoices from Moneybird, writes docs/data.json

DSO formula: (Total open AR / Total credit turnover last 180 days) x 180
Overdue %  : all invoices with days_overdue > 0 / total AR  (excl. bankrupt/bad_debt)
Score      : days_overdue * 1.0 + (amount/1000) * 0.3  (days weigh heavier)
"""

import argparse, datetime, json, math, os, subprocess, sys, time
import requests

MONEYBIRD_TOKEN   = os.getenv("MONEYBIRD_TOKEN",    "YOUR_TOKEN_HERE")
ADMINISTRATION_ID = os.getenv("MONEYBIRD_ADMIN_ID", "YOUR_ADMIN_ID_HERE")
DSO_TARGET        = int(os.getenv("DSO_TARGET", "36"))
OUT_DIR           = os.path.join(os.path.dirname(__file__), "docs")

BASE_URL = f"https://moneybird.com/api/v2/{ADMINISTRATION_ID}"
HEADERS  = {"Authorization": f"Bearer {MONEYBIRD_TOKEN}"}
TODAY    = datetime.date.today()
DAYS_180 = 180


def api_get(path, params=None):
    url, results = f"{BASE_URL}/{path}", []
    while url:
        r = requests.get(url, headers=HEADERS, params=params or {})
        if r.status_code == 401: sys.exit("Invalid Moneybird token.")
        if r.status_code == 404: sys.exit("Invalid Administration ID.")
        r.raise_for_status()
        d = r.json()
        if isinstance(d, list): results.extend(d)
        else: return d
        url = r.links.get("next", {}).get("url"); params = None
    return results


def fetch_open():
    """Open/late/reminded invoices (positive amounts = receivables)."""
    out = []
    for state in ["open", "late", "reminded"]:
        out.extend(api_get("sales_invoices.json", {"filter": f"state:{state}"}))
    return out


def fetch_sent_180():
    """All sent/paid invoices in last 180 days for DSO revenue base."""
    cutoff = (TODAY - datetime.timedelta(days=DAYS_180)).isoformat()
    out = []
    for state in ["sent", "open", "late", "reminded", "paid"]:
        try:
            batch = api_get("sales_invoices.json", {
                "filter": f"state:{state},invoice_date_after:{cutoff}"
            })
            out.extend(batch)
        except Exception:
            pass
    return out


def parse_reminder_date(inv):
    """Extract the date the latest reminder was sent from Moneybird."""
    # Moneybird stores send_reminders_count and payment_reminder_details
    reminders = inv.get("payment_reminder_details") or []
    if reminders:
        dates = [r.get("sent_at") or r.get("date") or "" for r in reminders]
        dates = [d[:10] for d in dates if d]
        if dates:
            return max(dates)
    # Fallback: updated_at if state is reminded
    if inv.get("state") == "reminded":
        updated = (inv.get("updated_at") or "")[:10]
        return updated or None
    return None


def parse(inv):
    c          = inv.get("contact") or {}
    due_str    = inv.get("due_date") or inv.get("invoice_date")
    due        = datetime.date.fromisoformat(due_str) if due_str else TODAY
    days       = max(0, (TODAY - due).days)
    amount     = float(inv.get("total_unpaid_base") or inv.get("total_price_incl_tax") or 0)
    company    = (c.get("company_name") or
                  f"{c.get('firstname','')} {c.get('lastname','')}".strip() or "Unknown")
    contact_id = str(inv.get("contact_id") or (c.get("id") if c else None) or "")

    # Moneybird state 'bad_debt' marks uncollectable invoices
    mb_state   = inv.get("state", "")
    is_bad_debt = mb_state == "bad_debt"

    def bucket(d):
        if d == 0:   return "Current"
        if d <= 30:  return "1-30 days"
        if d <= 60:  return "31-60 days"
        if d <= 90:  return "61-90 days"
        return ">90 days"

    # New score: days weigh heavier than amount
    score = round(days * 1.0 + (amount / 1000) * 0.3, 1)
    level = "URGENT" if score >= 70 else ("High" if score >= 40 else "Monitor")

    return {
        "id":              inv["id"],
        "contact_id":      contact_id,
        "invoice_no":      inv.get("invoice_id") or inv.get("reference") or inv["id"],
        "company":         company,
        "invoice_date":    inv.get("invoice_date", ""),
        "due_date":        due.isoformat(),
        "amount":          round(amount, 2),
        "currency":        inv.get("currency", "EUR"),
        "days_overdue":    days,
        "bucket":          bucket(days),
        "state":           mb_state,
        "is_bad_debt":     is_bad_debt,
        "score":           score,
        "level":           level,
        "reminder_date":   parse_reminder_date(inv),
    }


def build_data(open_invoices, sent_invoices):
    # ── DSO: Total open AR / (total credit sales last 180d / 180) x 180 ──
    total_ar = sum(i["amount"] for i in open_invoices if i["amount"] > 0)

    # Deduplicate sent invoices by id, sum their original invoice amounts
    seen = set()
    revenue_180 = 0.0
    for inv in sent_invoices:
        if inv["id"] in seen: continue
        seen.add(inv["id"])
        amt = float(inv.get("total_price_incl_tax") or 0)
        if amt > 0:
            revenue_180 += amt
    avg_daily = revenue_180 / DAYS_180 if revenue_180 else 1
    dso       = round(total_ar / avg_daily, 1) if avg_daily else 0

    # ── Exclude bad_debt from all KPIs (bankrupt exclusion done in frontend via flags) ──
    active = [i for i in open_invoices if not i["is_bad_debt"] and i["amount"] > 0]
    credit = [i for i in open_invoices if i["amount"] < 0]

    buckets = ["Current","1-30 days","31-60 days","61-90 days",">90 days"]
    aging   = {b: round(sum(i["amount"] for i in active if i["bucket"]==b), 2) for b in buckets}

    overdue = sorted(
        [i for i in active if i["days_overdue"] > 0],
        key=lambda x: x["score"], reverse=True
    )

    # ── Customer index ──
    customers = {}
    for inv in open_invoices:  # include all for customer panel
        cid = inv["contact_id"]
        if cid not in customers:
            customers[cid] = {"contact_id": cid, "company": inv["company"], "invoices": []}
        customers[cid]["invoices"].append(inv)

    for cid in customers:
        invs = customers[cid]["invoices"]
        invs.sort(key=lambda x: (-(x["days_overdue"] > 0), -x["amount"]))
        active_invs = [i for i in invs if not i["is_bad_debt"] and i["amount"] > 0]
        customers[cid]["total_open"]    = round(sum(i["amount"] for i in active_invs), 2)
        customers[cid]["total_overdue"] = round(sum(i["amount"] for i in active_invs if i["days_overdue"] > 0), 2)
        customers[cid]["max_days"]      = max((i["days_overdue"] for i in active_invs), default=0)
        customers[cid]["invoice_count"] = len(active_invs)
        top_score = max((i["score"] for i in active_invs if i["days_overdue"] > 0), default=0)
        customers[cid]["level"] = "URGENT" if top_score >= 70 else ("High" if top_score >= 40 else "Monitor")

    # ── History (for trend chart) ──
    hist_file = os.path.join(OUT_DIR, "history.json")
    history = []
    if os.path.exists(hist_file):
        with open(hist_file) as f:
            history = json.load(f)

    today_str    = TODAY.isoformat()
    overdue_amt  = sum(i["amount"] for i in active if i["days_overdue"] > 0)
    overdue_pct  = round(overdue_amt / sum(i["amount"] for i in active) * 100, 2) if active else 0

    history = [h for h in history if h["date"] != today_str]
    history.append({
        "date": today_str, "dso": dso, "target": DSO_TARGET,
        "overdue_pct": overdue_pct, "overdue_pct_target": 10
    })
    history = history[-180:]  # keep 180 days for 6-month avg
    with open(hist_file, "w") as f:
        json.dump(history, f)

    # ── 6-month averages for KPIs ──
    cutoff_6m    = (TODAY - datetime.timedelta(days=180)).isoformat()
    hist_6m      = [h for h in history if h["date"] >= cutoff_6m]
    avg_dso_6m   = round(sum(h["dso"] for h in hist_6m) / len(hist_6m), 1) if hist_6m else dso
    avg_pct_6m   = round(sum(h["overdue_pct"] for h in hist_6m) / len(hist_6m), 2) if hist_6m else overdue_pct

    active_total = sum(i["amount"] for i in active)

    return {
        "generated":      datetime.datetime.now().isoformat(),
        "dso":            dso,
        "dso_6m_avg":     avg_dso_6m,
        "dso_target":     DSO_TARGET,
        "total_ar":       round(active_total, 2),
        "invoice_count":  len(active),
        "overdue_pct":    overdue_pct,
        "overdue_pct_6m": avg_pct_6m,
        "overdue_30":     round(sum(i["amount"] for i in active if i["days_overdue"] > 30), 2),
        "overdue_60":     round(sum(i["amount"] for i in active if i["days_overdue"] > 60), 2),
        "overdue_90":     round(sum(i["amount"] for i in active if i["days_overdue"] > 90), 2),
        "aging":          aging,
        "history":        history,
        "invoices":       open_invoices,   # all invoices incl bad_debt for panels
        "active_invoices": active,         # filtered for KPIs
        "overdue_top":    overdue[:25],
        "customers":      customers,
        "credit_invoices": credit,
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
            print(f"  ok: {' '.join(cmd[2:])}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--push", action="store_true")
    parser.add_argument("--loop", type=int, default=0, metavar="HOURS")
    args = parser.parse_args()

    if MONEYBIRD_TOKEN == "YOUR_TOKEN_HERE":
        sys.exit("Set MONEYBIRD_TOKEN environment variable first.")

    def run():
        print(f"\n[{datetime.datetime.now().strftime('%H:%M')}] Fetching from Moneybird...")
        open_inv  = fetch_open()
        sent_inv  = fetch_sent_180()
        parsed    = [parse(i) for i in open_inv]
        data      = build_data(parsed, sent_inv)
        os.makedirs(OUT_DIR, exist_ok=True)
        with open(os.path.join(OUT_DIR, "data.json"), "w") as f:
            json.dump(data, f, indent=2)
        print(f"  data.json written ({len(parsed)} invoices, DSO: {data['dso']}d, 6m avg: {data['dso_6m_avg']}d)")
        if args.push:
            git_push()

    run()
    if args.loop:
        while True:
            time.sleep(args.loop * 3600)
            run()

if __name__ == "__main__":
    main()
