"""
generate_dashboard.py  v7
=========================
DSO = (gemiddeld debiteurensaldo 180d / totale omzet 180d) * 180
Overdue% = verlopen AR / totaal AR  (op dit moment)
Score = days_overdue * 1.0 + (amount / 1000) * 0.3
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
    out = []
    for state in ["open", "late", "reminded"]:
        out.extend(api_get("sales_invoices.json", {"filter": f"state:{state}"}))
    return out


def fetch_revenue_180():
    """Only PAID invoices in last 180 days for DSO denominator (actual revenue received)."""
    cutoff = (TODAY - datetime.timedelta(days=DAYS_180)).isoformat()
    out = []
    for state in ["paid"]:
        try:
            out.extend(api_get("sales_invoices.json", {
                "filter": f"state:{state},invoice_date_after:{cutoff}"
            }))
        except Exception:
            pass
    # Also include late/reminded/open sent in period as credit sales (sent = revenue earned)
    for state in ["sent", "open", "late", "reminded"]:
        try:
            out.extend(api_get("sales_invoices.json", {
                "filter": f"state:{state},invoice_date_after:{cutoff}"
            }))
        except Exception:
            pass
    return out


def parse_reminders(inv):
    """
    Build reminder timeline from Moneybird data.
    Moneybird sends: 1st reminder at day 6, 2nd at day 30, 3rd at day 60.
    We derive dates from due_date + known offsets, confirmed by state.
    Also try payment_reminder_details if available.
    """
    due_str  = inv.get("due_date") or inv.get("invoice_date")
    due      = datetime.date.fromisoformat(due_str) if due_str else TODAY
    state    = inv.get("state", "")
    reminders = []

    # Try real reminder data first
    details = inv.get("payment_reminder_details") or []
    if details:
        for i, d in enumerate(sorted(details, key=lambda x: x.get("sent_at",""))):
            sent = (d.get("sent_at") or d.get("date") or "")[:10]
            if sent:
                reminders.append({"nr": i+1, "date": sent, "source": "moneybird"})
        return reminders

    # Fallback: derive from due_date + state
    if state in ("late", "reminded"):
        r1_date = due + datetime.timedelta(days=6)
        if r1_date <= TODAY:
            reminders.append({"nr": 1, "date": r1_date.isoformat(), "source": "berekend"})
    if state == "reminded":
        days_overdue = (TODAY - due).days
        if days_overdue >= 30:
            r2_date = due + datetime.timedelta(days=30)
            reminders.append({"nr": 2, "date": r2_date.isoformat(), "source": "berekend"})
        if days_overdue >= 60:
            r3_date = due + datetime.timedelta(days=60)
            reminders.append({"nr": 3, "date": r3_date.isoformat(), "source": "berekend"})

    return reminders


def parse(inv):
    c          = inv.get("contact") or {}
    due_str    = inv.get("due_date") or inv.get("invoice_date")
    due        = datetime.date.fromisoformat(due_str) if due_str else TODAY
    days       = max(0, (TODAY - due).days)

    # Openstaand bedrag EX BTW
    # total_unpaid_base = openstaand ex btw (voorkeur)
    # total_price_excl_tax = factuurbedrag ex btw (fallback)
    amount_raw = (
        inv.get("total_unpaid_base") or
        inv.get("total_price_excl_tax") or
        inv.get("total_price_incl_tax") or 0
    )
    amount = float(amount_raw)

    company    = (c.get("company_name") or
                  f"{c.get('firstname','')} {c.get('lastname','')}".strip() or "Unknown")
    contact_id = str(inv.get("contact_id") or (c.get("id") if c else None) or "")
    mb_state   = inv.get("state", "")
    is_bad_debt = mb_state == "bad_debt"

    def bucket(d):
        if d == 0:   return "Current"
        if d <= 30:  return "1-30 days"
        if d <= 60:  return "31-60 days"
        if d <= 90:  return "61-90 days"
        return ">90 days"

    # Score: days_overdue * 1.0 + (amount / 1000) * 0.3
    score = round(days * 1.0 + (abs(amount) / 1000) * 0.3, 1)
    level = "URGENT" if score >= 70 else ("High" if score >= 40 else "Monitor")

    return {
        "id":           inv["id"],
        "contact_id":   contact_id,
        "invoice_no":   inv.get("invoice_id") or inv.get("reference") or inv["id"],
        "company":      company,
        "invoice_date": inv.get("invoice_date", ""),
        "due_date":     due.isoformat(),
        "amount":       round(amount, 2),
        "currency":     inv.get("currency", "EUR"),
        "days_overdue": days,
        "bucket":       bucket(days),
        "state":        mb_state,
        "is_bad_debt":  is_bad_debt,
        "score":        score,
        "level":        level,
        "reminders":    parse_reminders(inv),
    }


def build_data(open_invoices, revenue_invoices):
    # ── History: load ──
    hist_file = os.path.join(OUT_DIR, "history.json")
    history = []
    if os.path.exists(hist_file):
        with open(hist_file) as f:
            history = json.load(f)

    # Active = exclude bad_debt. Credits (negatief) worden WEL meegenomen in totaal AR.
    active      = [i for i in open_invoices if not i["is_bad_debt"] and i["amount"] > 0]
    credit      = [i for i in open_invoices if i["amount"] < 0]
    active_excl_credit = active  # voor KPIs en aging alleen positieve facturen

    # Totaal AR incl. creditfacturen (netto debiteurenstand)
    total_ar_gross = round(sum(i["amount"] for i in active), 2)
    total_ar_credit = round(sum(i["amount"] for i in credit), 2)   # negatief getal
    total_ar       = round(total_ar_gross + total_ar_credit, 2)    # netto

    overdue_ar  = round(sum(i["amount"] for i in active if i["days_overdue"] > 0), 2)
    overdue_pct = round(overdue_ar / total_ar * 100, 2) if total_ar else 0.0

    # ── Revenue 180d ex BTW (deduplicated) ──
    seen_ids    = set()
    revenue_180 = 0.0
    for inv in revenue_invoices:
        if inv["id"] in seen_ids: continue
        seen_ids.add(inv["id"])
        # Gebruik ex-btw bedrag
        amt = float(inv.get("total_price_excl_tax") or inv.get("total_price_incl_tax") or 0)
        if amt > 0:
            revenue_180 += amt

    # ── DSO vandaag: (netto debiteurenstand ex BTW / omzet 180d ex BTW) * 180 ──
    dso = round((total_ar / revenue_180 * DAYS_180), 1) if revenue_180 else 0.0

    # ── Update history with today's snapshot ──
    today_str = TODAY.isoformat()
    history = [h for h in history if h["date"] != today_str]
    history.append({
        "date":        today_str,
        "total_ar":    total_ar,
        "overdue_ar":  overdue_ar,
        "overdue_pct": overdue_pct,
        "dso":         dso,
        "target":      DSO_TARGET,
    })
    history = sorted(history, key=lambda h: h["date"])[-365:]

    # ── 6-month averages ──
    cutoff_6m = (TODAY - datetime.timedelta(days=180)).isoformat()
    hist_6m   = [h for h in history if h["date"] >= cutoff_6m]
    hist_6m_dso = [h for h in hist_6m if h.get("dso", 0) > 0]
    avg_dso_6m = round(sum(h["dso"] for h in hist_6m_dso) / len(hist_6m_dso), 1) if hist_6m_dso else dso
    avg_pct_6m = round(sum(h.get("overdue_pct", overdue_pct) for h in hist_6m) / len(hist_6m), 2) if hist_6m else overdue_pct

    # ── Write history.json separately ──
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(hist_file, "w") as f:
        json.dump(history, f, indent=2)

    # ── Aging (alleen actieve, positieve facturen) ──
    buckets = ["Current","1-30 days","31-60 days","61-90 days",">90 days"]
    aging   = {b: round(sum(i["amount"] for i in active if i["bucket"]==b), 2) for b in buckets}

    overdue_sorted = sorted(
        [i for i in active if i["days_overdue"] > 0],
        key=lambda x: x["score"], reverse=True
    )

    # ── Customer index ──
    customers = {}
    for inv in open_invoices:
        cid = inv["contact_id"]
        if cid not in customers:
            customers[cid] = {"contact_id": cid, "company": inv["company"], "invoices": []}
        customers[cid]["invoices"].append(inv)

    for cid in customers:
        invs = customers[cid]["invoices"]
        invs.sort(key=lambda x: (-(x["days_overdue"] > 0), -x["amount"]))
        act  = [i for i in invs if not i["is_bad_debt"] and i["amount"] > 0]
        customers[cid]["total_open"]    = round(sum(i["amount"] for i in act), 2)
        customers[cid]["total_overdue"] = round(sum(i["amount"] for i in act if i["days_overdue"] > 0), 2)
        customers[cid]["max_days"]      = max((i["days_overdue"] for i in act), default=0)
        customers[cid]["invoice_count"] = len(act)
        top = max((i["score"] for i in act if i["days_overdue"] > 0), default=0)
        customers[cid]["level"] = "URGENT" if top >= 70 else ("High" if top >= 40 else "Monitor")

    return {
        "generated":       datetime.datetime.now().isoformat(),
        "dso":             dso,
        "dso_6m_avg":      avg_dso_6m,
        "dso_target":      DSO_TARGET,
        "total_ar":        total_ar,
        "total_ar_gross":  total_ar_gross,
        "total_ar_credit": total_ar_credit,
        "invoice_count":   len(active),
        "overdue_pct":     overdue_pct,
        "overdue_pct_6m":  avg_pct_6m,
        "overdue_30":      round(sum(i["amount"] for i in active if i["days_overdue"] > 30), 2),
        "overdue_60":      round(sum(i["amount"] for i in active if i["days_overdue"] > 60), 2),
        "overdue_90":      round(sum(i["amount"] for i in active if i["days_overdue"] > 90), 2),
        "aging":           aging,
        "history":         history,
        "invoices":        open_invoices,
        "active_invoices": active,
        "overdue_top":     overdue_sorted[:25],
        "customers":       customers,
        "credit_invoices": credit,
    }


def git_push():
    for cmd in [
        ["git", "-C", OUT_DIR+"/..", "add", "docs/"],
        ["git", "-C", OUT_DIR+"/..", "commit", "-m", f"chore: DSO sync {TODAY.isoformat()}"],
        ["git", "-C", OUT_DIR+"/..", "push"],
    ]:
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0 and "nothing to commit" not in r.stdout:
            print(f"  git: {r.stderr.strip() or r.stdout.strip()}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--push", action="store_true")
    parser.add_argument("--loop", type=int, default=0)
    args = parser.parse_args()

    if MONEYBIRD_TOKEN in ("YOUR_TOKEN_HERE", "", None):
        sys.exit("ERROR: MONEYBIRD_TOKEN secret is not set in GitHub repository settings.")
    if ADMINISTRATION_ID in ("YOUR_ADMIN_ID_HERE", "", None):
        sys.exit("ERROR: MONEYBIRD_ADMIN_ID secret is not set in GitHub repository settings.")

    def run():
        print(f"\n[{datetime.datetime.now().strftime('%H:%M')}] Fetching from Moneybird...")
        open_inv = fetch_open()
        rev_inv  = fetch_revenue_180()
        parsed   = [parse(i) for i in open_inv]
        data     = build_data(parsed, rev_inv)
        os.makedirs(OUT_DIR, exist_ok=True)
        with open(os.path.join(OUT_DIR, "data.json"), "w") as f:
            json.dump(data, f, indent=2)
        print(f"  data.json: {len(parsed)} invoices, DSO {data['dso']}d (6m avg: {data['dso_6m_avg']}d), overdue {data['overdue_pct']}%")
        if args.push:
            git_push()

    run()
    if args.loop:
        while True:
            time.sleep(args.loop * 3600)
            run()

if __name__ == "__main__":
    main()
