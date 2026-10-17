"""
Generates the files that the source systems of "Stride Sports" drop every day in the landing zone.

One call to generate_batch(landing_date) writes everything that *arrives* on that date:

    erp/stores/stores_YYYYMMDD.csv               full extract (a new store appears, then opens)
    erp/products/products_YYYYMMDD.csv           full extract (prices change: promos, price increase)
    crm/customers/customers_YYYYMMDD.json        change feed (I = insert, U = update), full row image
    crm/gdpr_requests/gdpr_requests_YYYYMMDD.csv right-to-erasure requests
    pos/sales/pos_sales_<store>_YYYYMMDD.csv     one file per store and business day (local time, local currency)
    ecom/orders/orders_YYYYMMDD.json             order snapshots, one per status change (CDC), nested lines
    ecom/returns/returns_YYYYMMDD.csv            web returns
    supply/inventory/inventory_YYYYMMDD.csv      end-of-day stock per store and product
    finance/fx_rates/fx_rates_YYYYMMDD.json      ECB-like rates, business days only

Realistic defects are built in (disable them with clean=True): duplicate lines, files re-sent, files
arriving one day late, unknown products, zero quantities, negative prices, blank store ids, late web
events, guest checkouts (null customer), negative stock, schema drift on fixed dates.
"""

from __future__ import annotations

import csv
import json
import math
import random
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config as C

UTC = timezone.utc


def rng(*parts) -> random.Random:
    """Deterministic random generator: the same parts always give the same sequence."""
    return random.Random("|".join(str(p) for p in parts))


def day_index(d: date) -> int:
    return (d - C.EPOCH).days


def ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def iso_utc(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def psychological(price: float) -> float:
    """49.37 -> 49.99, the way retail prices are displayed."""
    return round(max(math.floor(price), 0) + 0.99, 2)


# ---------------------------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------------------------

@lru_cache(maxsize=1)
def catalogue() -> tuple[dict, ...]:
    r = rng("catalogue")
    products = []
    n = 100000
    for sport, (brand, categories, _season) in C.SPORTS.items():
        for category, (lo, hi) in categories.items():
            for i in range(C.PRODUCTS_PER_CATEGORY):
                n += 1
                tier = (100, 300, 500, 900)[i * 4 // C.PRODUCTS_PER_CATEGORY]
                base = lo + (hi - lo) * (i + r.random()) / C.PRODUCTS_PER_CATEGORY
                launched_before = r.random() < 0.85
                launch = (C.EPOCH - timedelta(days=r.randint(30, 1500)) if launched_before
                          else C.EPOCH + timedelta(days=r.randint(1, 120)))
                discontinued = C.EPOCH + timedelta(days=r.randint(20, 150)) if r.random() < 0.05 else None
                products.append({
                    "sku": f"SKU{n:06d}",
                    "product_name": f"{brand} {category.replace('-', ' ').title()} {tier} {chr(65 + i)}",
                    "sport": sport,
                    "category": category,
                    "brand": brand,
                    "base_price": psychological(base),
                    "cost_ratio": round(r.uniform(0.38, 0.62), 3),
                    "launch_date": launch,
                    "discontinued_date": discontinued,
                    "price_increase": r.random() < 0.20,
                })
    return tuple(products)


def in_promo(sku: str, d: date) -> bool:
    year, week, _ = d.isocalendar()
    return rng("promo", sku, year, week).random() < 0.04


def price_eur(p: dict, d: date) -> float:
    price = p["base_price"]
    if p["price_increase"] and d >= C.PRICE_INCREASE_DATE:
        price = psychological(price * 1.05)
    if in_promo(p["sku"], d):
        price = round(price * 0.8, 2)
    return price


def product_status(p: dict, d: date) -> str:
    return "discontinued" if p["discontinued_date"] and d >= p["discontinued_date"] else "active"


def product_last_change(p: dict, d: date) -> date:
    """Last date the ERP record changed (price or status), as an ERP 'last_modified' column."""
    candidates = [p["launch_date"]]
    if p["price_increase"] and C.PRICE_INCREASE_DATE <= d:
        candidates.append(C.PRICE_INCREASE_DATE)
    if p["discontinued_date"] and p["discontinued_date"] <= d:
        candidates.append(p["discontinued_date"])
    monday = d - timedelta(days=d.weekday())
    for _ in range(30):  # promo state changes happen on Mondays
        if monday <= p["launch_date"]:
            break
        if in_promo(p["sku"], monday) != in_promo(p["sku"], monday - timedelta(days=7)):
            candidates.append(monday)
            break
        monday -= timedelta(days=7)
    return max(c for c in candidates if c <= d)


def active_products(d: date) -> list[dict]:
    return [p for p in catalogue()
            if p["launch_date"] <= d and (p["discontinued_date"] is None or p["discontinued_date"] > d)]


def product_weights(products: list[dict], d: date) -> list[float]:
    return [C.SPORTS[p["sport"]][2][d.month - 1] / max(p["base_price"], 5) ** 0.35 for p in products]


# ---------------------------------------------------------------------------------------------
# FX rates
# ---------------------------------------------------------------------------------------------

def fx_rate(currency: str, d: date) -> float:
    if currency == "EUR":
        return 1.0
    drift = 0.015 * math.sin(day_index(d) / 23 + len(currency)) + rng("fx", currency, d).uniform(-0.002, 0.002)
    return round(C.FX_BASE[currency] * (1 + drift), 4)


def last_business_day(d: date) -> date:
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def local_price(p: dict, d: date, currency: str) -> float:
    eur = price_eur(p, d)
    if currency == "EUR":
        return eur
    return psychological(eur * fx_rate(currency, last_business_day(d)))


# ---------------------------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------------------------

def store_dict(s: tuple) -> dict:
    store_id, name, city, country, tz, currency, size, opening = s
    return {"store_id": store_id, "store_name": name, "city": city, "country": country, "timezone": tz,
            "currency": currency, "size": size, "opening_date": date.fromisoformat(opening)}


STORES = [store_dict(s) for s in C.STORES]


def store_is_open(s: dict, d: date) -> bool:
    return d >= s["opening_date"]


# ---------------------------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------------------------

def new_customers_count(d: date) -> int:
    return rng("new_customers", d).randint(15, 40)


def customer_signup(cid: int) -> date:
    if cid <= C.INITIAL_CUSTOMERS:
        return C.EPOCH - timedelta(days=rng(cid, "signup").randint(1, 2900))
    return C.EPOCH + timedelta(days=(cid - C.INITIAL_CUSTOMERS - 1) // 100)


def random_existing_customer(r: random.Random, d: date) -> int:
    """A customer id that already exists on date d."""
    if day_index(d) <= 0 or r.random() < 0.8:
        return r.randint(1, C.INITIAL_CUSTOMERS)
    day = r.randint(0, day_index(d) - 1)
    created = C.EPOCH + timedelta(days=day)
    return C.INITIAL_CUSTOMERS + day * 100 + r.randint(1, new_customers_count(created))


def customer_country(cid: int) -> str:
    countries, weights = zip(*C.CUSTOMER_COUNTRY_WEIGHTS.items())
    return rng(cid, "country").choices(countries, weights)[0]


def customer_record(cid: int, version: date, op: str) -> dict:
    base = rng(cid, "base")
    country = customer_country(cid)
    first = base.choice(C.FIRST_NAMES[country])
    last = base.choice(C.LAST_NAMES[country])
    mut = rng(cid, "mutable", version)
    city, postal_code = mut.choice(C.CITIES[country])
    updated = datetime.combine(version, time(6), UTC) + timedelta(seconds=mut.randint(0, 16 * 3600))
    return {
        "op": op,
        "customer_id": f"C{cid:07d}",
        "first_name": first,
        "last_name": last,
        "email": f"{first}.{last}{mut.randint(1, 999)}@example.com".lower(),
        "birth_year": base.randint(1950, 2008),
        "country": country,
        "city": city,
        "postal_code": postal_code,
        "loyalty_tier": mut.choices(["bronze", "silver", "gold"], [70, 22, 8])[0],
        "marketing_opt_in": mut.random() < 0.55,
        "signup_date": customer_signup(cid).isoformat(),
        "updated_at": iso_utc(updated),
    }


def customer_changes(d: date) -> list[dict]:
    if d == C.EPOCH:  # initial full load from the CRM
        rows = [customer_record(cid, customer_signup(cid), "I") for cid in range(1, C.INITIAL_CUSTOMERS + 1)]
    else:
        rows = []
    first_new = C.INITIAL_CUSTOMERS + day_index(d) * 100 + 1
    rows += [customer_record(cid, d, "I") for cid in range(first_new, first_new + new_customers_count(d))]
    if d > C.EPOCH:
        r = rng("customer_updates", d)
        updated = {random_existing_customer(r, d) for _ in range(r.randint(8, 20))}
        rows += [customer_record(cid, d, "U") for cid in sorted(updated)]
    return rows


def gdpr_requests(d: date) -> list[dict]:
    r = rng("gdpr", d)
    rows = []
    for i in range(r.choice([0, 0, 1, 1, 2, 3])):
        ts = datetime.combine(d, time(8), UTC) + timedelta(minutes=r.randint(0, 600))
        rows.append({"request_id": f"GDPR-{ymd(d)}-{i + 1}",
                     "customer_id": f"C{random_existing_customer(r, d):07d}",
                     "request_type": "erasure",
                     "requested_at": iso_utc(ts)})
    return rows


# ---------------------------------------------------------------------------------------------
# Point of sale (stores)
# ---------------------------------------------------------------------------------------------

STORE_WEEKDAY = [0.9, 0.85, 0.9, 1.0, 1.25, 1.6, 0.55]  # Monday .. Sunday
STORE_HOURS = list(range(9, 20))
STORE_HOUR_WEIGHTS = [3, 5, 7, 9, 8, 6, 6, 8, 10, 9, 5]


def pos_columns(d: date) -> list[str]:
    cols = ["transaction_id", "line_number", "store_id", "transaction_type", "sku", "quantity", "unit_price",
            "discount_amount", "currency", "transaction_ts", "payment_method", "customer_id", "cashier_id"]
    if d >= C.POS_UPGRADE_DATE:
        cols.append("loyalty_points")
    return cols


def pos_lines(s: dict, d: date, clean: bool) -> list[dict]:
    r = rng("pos", s["store_id"], d)
    products = active_products(d)
    weights = product_weights(products, d)
    tz = ZoneInfo(s["timezone"])
    v2 = d >= C.POS_UPGRADE_DATE
    n_tx = max(0, int(r.gauss(110 * s["size"] * STORE_WEEKDAY[d.weekday()], 12 * s["size"])))
    rows = []
    for t in range(n_tx):
        local_ts = datetime.combine(d, time(r.choices(STORE_HOURS, STORE_HOUR_WEIGHTS)[0], r.randint(0, 59),
                                            r.randint(0, 59)))
        is_return = r.random() < 0.03
        n_lines = 1 if is_return else r.choices([1, 2, 3, 4], [55, 28, 12, 5])[0]
        customer = f"C{random_existing_customer(r, d):07d}" if r.random() < 0.45 else ""
        payment = r.choices(["card", "cash", "mobile"], [70, 15, 15])[0]
        cashier = f"{s['store_id']}-K{r.randint(1, 25):02d}"
        ts_text = (local_ts.replace(tzinfo=tz).isoformat(timespec="seconds") if v2
                   else local_ts.strftime("%Y-%m-%d %H:%M:%S"))
        for line_no, p in enumerate(r.choices(products, weights, k=n_lines), start=1):
            qty = -1 if is_return else r.choices([1, 2, 3], [85, 12, 3])[0]
            unit_price = local_price(p, d, s["currency"])
            discount = round(unit_price * qty * r.choice([0.1, 0.15, 0.2]), 2) if r.random() < 0.08 and qty > 0 else 0
            row = {"transaction_id": f"{s['store_id']}-{ymd(d)}-{t + 1:05d}", "line_number": line_no,
                   "store_id": s["store_id"], "transaction_type": "RETURN" if is_return else "SALE",
                   "sku": p["sku"], "quantity": qty, "unit_price": f"{unit_price:.2f}",
                   "discount_amount": f"{discount:.2f}", "currency": s["currency"], "transaction_ts": ts_text,
                   "payment_method": payment, "customer_id": customer, "cashier_id": cashier}
            if v2:
                row["loyalty_points"] = int(unit_price * qty) if customer else ""
            if not clean:
                x = r.random()
                if x < 0.003:
                    row["sku"] = C.UNKNOWN_SKU                 # product not (yet) in the ERP extract
                elif x < 0.005:
                    row["quantity"] = 0                        # scanning error
                elif x < 0.006:
                    row["unit_price"] = f"{-unit_price:.2f}"   # sign error in the POS export
                elif x < 0.007:
                    row["store_id"] = ""                       # missing store id
            rows.append(row)
            if not clean and r.random() < 0.002:
                rows.append(dict(row))                         # line exported twice
    return rows


def pos_file_is_late(s: dict, d: date, clean: bool) -> bool:
    return not clean and rng("pos_late", s["store_id"], d).random() < 0.015


def pos_file_is_resent(s: dict, d: date, clean: bool) -> bool:
    return not clean and rng("pos_resend", s["store_id"], d).random() < 0.02


# ---------------------------------------------------------------------------------------------
# E-commerce orders and returns
# ---------------------------------------------------------------------------------------------

WEB_WEEKDAY = [1.1, 1.0, 1.0, 0.95, 0.9, 0.85, 1.2]
WEB_HOURS = list(range(24))
WEB_HOUR_WEIGHTS = [2, 1, 1, 1, 1, 1, 2, 4, 5, 6, 6, 7, 8, 7, 6, 6, 6, 7, 9, 11, 12, 11, 8, 4]


@lru_cache(maxsize=64)
def orders_created_on(d: date) -> tuple[dict, ...]:
    """Every web order created on d (UTC), with its full status history."""
    r = rng("orders", d)
    products = active_products(d)
    weights = product_weights(products, d)
    countries, cweights = zip(*C.CUSTOMER_COUNTRY_WEIGHTS.items())
    n = max(0, int(r.gauss(650 * WEB_WEEKDAY[d.weekday()], 40)))
    orders = []
    for i in range(n):
        order_id = f"W{ymd(d)}{i + 1:05d}"
        ro = rng(order_id)
        country = ro.choices(countries, cweights)[0]
        currency = C.COUNTRY_CURRENCY[country]
        guest = ro.random() < 0.25
        created = datetime.combine(d, time(ro.choices(WEB_HOURS, WEB_HOUR_WEIGHTS)[0], ro.randint(0, 59),
                                           ro.randint(0, 59)), UTC)
        lines = []
        for k, p in enumerate(ro.choices(products, weights, k=ro.choices([1, 2, 3, 4, 5], [45, 28, 15, 8, 4])[0])):
            lines.append({"line_id": f"{order_id}-{k + 1}", "sku": p["sku"],
                          "quantity": ro.choices([1, 2], [90, 10])[0],
                          "unit_price": local_price(p, d, currency)})
        pickup_stores = [s["store_id"] for s in STORES if s["country"] == country and store_is_open(s, d)]
        method = ro.choices(["home", "relay", "click_and_collect"], [55, 30, 15])[0]
        if method == "click_and_collect" and not pickup_stores:
            method = "home"
        shipping = {"method": method,
                    "pickup_store_id": ro.choice(pickup_stores) if method == "click_and_collect" else None,
                    "cost": {"home": 4.99, "relay": 2.99, "click_and_collect": 0.0}[method]}
        events = [("CREATED", created)]
        if ro.random() < 0.03:
            events.append(("CANCELLED", created + timedelta(minutes=ro.randint(5, 600))))
        else:
            paid = created + timedelta(minutes=ro.randint(1, 20))
            shipped = paid + timedelta(hours=ro.randint(18, 60))
            delivered = shipped + timedelta(hours=ro.randint(30, 96))
            events += [("PAID", paid), ("SHIPPED", shipped), ("DELIVERED", delivered)]
        orders.append({
            "order_id": order_id,
            "customer_id": None if guest else f"C{random_existing_customer(ro, d):07d}",
            "country": country, "currency": currency, "created_at": created, "lines": lines,
            "shipping": shipping,
            "promo_code": ro.choice(["WELCOME10", "SPORT20", "RENTREE15"]) if ro.random() < 0.1 else None,
            "device": ro.choices(["web", "ios", "android"], [45, 30, 25])[0],
            # 4% of status events reach the data platform 1 to 3 days late
            "events": [(status, ts, ro.randint(1, 3) if ro.random() < 0.04 else 0) for status, ts in events],
        })
    return tuple(orders)


def order_snapshot(o: dict, status: str, ts: datetime) -> dict:
    record = {"order_id": o["order_id"], "customer_id": o["customer_id"], "order_status": status,
              "created_at": iso_utc(o["created_at"]), "updated_at": iso_utc(ts), "country": o["country"],
              "currency": o["currency"], "promo_code": o["promo_code"], "shipping": o["shipping"],
              "lines": o["lines"]}
    if ts.date() >= C.WEB_DEVICE_FIELD_DATE:
        record["device"] = o["device"]
    return record


def web_events_landing_on(landing: date, clean: bool) -> list[dict]:
    rows = []
    for back in range(0, 14):
        d = landing - timedelta(days=back)
        if d < C.EPOCH:
            break
        for o in orders_created_on(d):
            for status, ts, delay in o["events"]:
                if ts.date() + timedelta(days=0 if clean else delay) == landing:
                    rows.append(order_snapshot(o, status, ts))
    return rows


def web_returns_landing_on(landing: date) -> list[dict]:
    rows = []
    for back in range(0, 45):
        d = landing - timedelta(days=back)
        if d < C.EPOCH:
            break
        for o in orders_created_on(d):
            delivered = [ts for status, ts, _ in o["events"] if status == "DELIVERED"]
            if not delivered:
                continue
            rr = rng(o["order_id"], "return")
            if rr.random() >= 0.09:
                continue
            requested = delivered[0] + timedelta(days=rr.randint(2, 25), hours=rr.randint(0, 12))
            if requested.date() != landing:
                continue
            for k, line in enumerate(rr.sample(o["lines"], k=1 if len(o["lines"]) == 1 else rr.randint(1, 2))):
                rows.append({"return_id": f"R{o['order_id'][1:]}{k + 1}", "order_id": o["order_id"],
                             "line_id": line["line_id"], "sku": line["sku"], "quantity": line["quantity"],
                             "reason": rr.choice(C.RETURN_REASONS), "requested_at": iso_utc(requested),
                             "return_channel": rr.choices(["mail", "store"], [65, 35])[0],
                             "refund_amount": f"{line['unit_price'] * line['quantity']:.2f}",
                             "currency": o["currency"]})
    return rows


# ---------------------------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------------------------

def inventory_rows(d: date, clean: bool) -> list[dict]:
    year, week, _ = d.isocalendar()
    extracted = iso_utc(datetime.combine(d, time(22, 30), UTC))
    rows = []
    for s in STORES:
        if not store_is_open(s, d):
            continue
        for p in active_products(d):
            r = rng("inventory", s["store_id"], p["sku"], d)
            level = rng("inventory_week", s["store_id"], p["sku"], year, week).randint(0, int(40 * s["size"]))
            on_hand = 0 if r.random() < 0.02 else max(0, level + r.randint(-5, 5))
            if not clean and r.random() < 0.0005:
                on_hand = -r.randint(1, 5)                     # booking error in the stock system
            reserved = min(on_hand, r.randint(0, 2)) if on_hand > 0 else 0
            rows.append({"snapshot_date": d.isoformat(), "store_id": s["store_id"], "sku": p["sku"],
                         "quantity_on_hand": on_hand, "quantity_reserved": reserved, "extracted_at": extracted})
    return rows


# ---------------------------------------------------------------------------------------------
# Writing a landing batch
# ---------------------------------------------------------------------------------------------

def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def store_extract(landing: date) -> list[dict]:
    """A store appears in the ERP 14 days before it opens, with status 'planned'."""
    rows = []
    for s in STORES:
        announced = s["opening_date"] - timedelta(days=14)
        if announced > landing:
            continue
        last_change = s["opening_date"] if store_is_open(s, landing) else announced
        rows.append({"store_id": s["store_id"], "store_name": s["store_name"], "city": s["city"],
                     "country": s["country"], "timezone": s["timezone"], "currency": s["currency"],
                     "opening_date": s["opening_date"].isoformat(),
                     "status": "open" if store_is_open(s, landing) else "planned",
                     "surface_m2": int(1500 * s["size"] + 400),
                     "updated_at": iso_utc(datetime.combine(last_change, time(1), UTC))})
    return rows


def generate_batch(landing: date, out_dir: Path, clean: bool = False, incidents: tuple[str, ...] = ()) -> list[str]:
    """Write every file landing on `landing` under out_dir. Returns the relative paths written."""
    if landing < C.EPOCH:
        raise ValueError(f"{landing} is before the start of history ({C.EPOCH})")
    unknown = set(incidents) - set(C.INCIDENTS)
    if unknown:
        raise ValueError(f"Unknown incident(s) {sorted(unknown)}. Available: {sorted(C.INCIDENTS)}")
    out_dir = Path(out_dir)
    written: list[str] = []
    stamp = ymd(landing)

    def track(path: Path) -> None:
        written.append(path.relative_to(out_dir).as_posix())

    # ERP
    path = out_dir / "erp/stores" / f"stores_{stamp}.csv"
    write_csv(path, ["store_id", "store_name", "city", "country", "timezone", "currency", "opening_date", "status",
                     "surface_m2", "updated_at"], store_extract(landing))
    track(path)

    factor = 100 if "price_x100" in incidents else 1
    products = [{"sku": p["sku"], "product_name": p["product_name"], "sport": p["sport"], "category": p["category"],
                 "brand": p["brand"], "unit_price_eur": f"{price_eur(p, landing) * factor:.2f}",
                 "unit_cost_eur": f"{p['base_price'] * p['cost_ratio']:.2f}", "status": product_status(p, landing),
                 "launch_date": p["launch_date"].isoformat(),
                 "last_modified": product_last_change(p, landing).isoformat()}
                for p in catalogue() if p["launch_date"] <= landing]
    path = out_dir / "erp/products" / f"products_{stamp}.csv"
    write_csv(path, list(products[0]), products)
    track(path)

    # CRM
    path = out_dir / "crm/customers" / f"customers_{stamp}.json"
    write_jsonl(path, customer_changes(landing))
    track(path)
    path = out_dir / "crm/gdpr_requests" / f"gdpr_requests_{stamp}.csv"
    write_csv(path, ["request_id", "customer_id", "request_type", "requested_at"], gdpr_requests(landing))
    track(path)

    # POS: today's files when on time, plus yesterday's files that were late
    if "pos_outage" not in incidents:
        for s in STORES:
            for business_day in (landing - timedelta(days=1), landing):
                if business_day < C.EPOCH or not store_is_open(s, business_day):
                    continue
                if (business_day == landing) == pos_file_is_late(s, business_day, clean):
                    continue
                columns = pos_columns(business_day)
                empty = "empty_pos_file" in incidents and s["store_id"] == "ST003"
                rows = [] if empty else pos_lines(s, business_day, clean)
                if "schema_break" in incidents:
                    columns = ["qty" if c == "quantity" else c for c in columns]
                    rows = [{("qty" if k == "quantity" else k): v for k, v in row.items()} for row in rows]
                name = f"pos_sales_{s['store_id']}_{ymd(business_day)}"
                copies = [name]
                if pos_file_is_resent(s, business_day, clean):
                    copies.append(f"{name}_resend")
                if "duplicate_batch" in incidents:
                    copies.append(f"{name}_dup")
                for copy_name in copies:
                    path = out_dir / "pos/sales" / f"{copy_name}.csv"
                    write_csv(path, columns, rows)
                    track(path)

    # E-commerce
    events = web_events_landing_on(landing, clean)
    if "guest_checkout_flood" in incidents:
        r = rng("guest_flood", landing)
        events = [{**e, "customer_id": None} if r.random() < 0.8 else e for e in events]
    path = out_dir / "ecom/orders" / f"orders_{stamp}.json"
    write_jsonl(path, events)
    track(path)
    path = out_dir / "ecom/returns" / f"returns_{stamp}.csv"
    write_csv(path, ["return_id", "order_id", "line_id", "sku", "quantity", "reason", "requested_at", "return_channel",
                     "refund_amount", "currency"], web_returns_landing_on(landing))
    track(path)

    # Supply chain
    path = out_dir / "supply/inventory" / f"inventory_{stamp}.csv"
    write_csv(path, ["snapshot_date", "store_id", "sku", "quantity_on_hand", "quantity_reserved", "extracted_at"],
              inventory_rows(landing, clean))
    track(path)

    # Finance: FX rates are published on business days only
    if landing.weekday() < 5:
        path = out_dir / "finance/fx_rates" / f"fx_rates_{stamp}.json"
        write_jsonl(path, [{"base_currency": "EUR", "rate_date": landing.isoformat(),
                            "rates": {c: fx_rate(c, landing) for c in C.FX_BASE},
                            "published_at": iso_utc(datetime.combine(landing, time(14, 15), UTC))}])
        track(path)

    return written


def generate_range(start: date, end: date, out_dir: Path, clean: bool = False) -> list[str]:
    written = []
    d = start
    while d <= end:
        written += generate_batch(d, out_dir, clean=clean)
        d += timedelta(days=1)
    return written
