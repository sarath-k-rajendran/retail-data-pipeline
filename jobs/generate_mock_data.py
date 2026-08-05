#!/usr/bin/env python3
"""Local mock data generator for the retail Medallion pipeline.

Generates CSV source files that mimic the raw feeds landing in S3 raw/:
product, store, promotion, orders, returns, web_sessions, inventory_snapshot.

Design notes (see plan.md Phase 0/1):
- product/store/promotion are always emitted as full reference snapshots,
  in both --mode full and --mode delta, since real retail master data feeds
  are small enough to resend in full each load.
- orders/returns/web_sessions/inventory_snapshot are true incremental facts:
  --mode full emits a full historical date range; --mode delta emits only
  --as-of-date plus a small number of "correction" rows that reuse business
  keys from the prior day, to exercise the Silver upsert (INSERT+UPDATE) path.
- All dimension IDs are deterministic (sequential, not random) so that a
  delta run's references to "existing" products/stores are always valid
  without needing state from a prior run.
- A small, configurable percentage of rows are deliberately corrupted
  (nulls in required fields, bad enums/dates, duplicate business keys,
  orphan foreign keys) to exercise Bronze CSV parse modes and Silver
  validation/quarantine logic. A few hand-crafted malformed CSV lines are
  appended directly to orders.csv to exercise PERMISSIVE/DROPMALFORMED/
  FAILFAST parsing.
"""
from __future__ import annotations

import argparse
import csv
import os
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta

# --------------------------------------------------------------------------
# Reference pools
# --------------------------------------------------------------------------

CATEGORIES = {
    "Apparel": ["Mens", "Womens", "Kids"],
    "Electronics": ["Mobile", "Audio", "Computing"],
    "Home": ["Kitchen", "Furniture", "Decor"],
    "Grocery": ["Snacks", "Beverages", "Produce"],
    "Beauty": ["Skincare", "Haircare", "Makeup"],
}
BRANDS = [
    "Acme", "Northwind", "Contoso", "Globex", "Initech",
    "Umbrella", "Stark", "Wayne", "Hooli", "Vandelay",
]
CITIES_BY_REGION = {
    "Northeast": [("New York", "NY"), ("Boston", "MA"), ("Newark", "NJ")],
    "Southeast": [("Miami", "FL"), ("Atlanta", "GA"), ("Charlotte", "NC")],
    "Midwest": [("Chicago", "IL"), ("Columbus", "OH"), ("Detroit", "MI")],
    "West": [("Los Angeles", "CA"), ("Seattle", "WA"), ("Portland", "OR")],
    "Southwest": [("Houston", "TX"), ("Phoenix", "AZ"), ("Albuquerque", "NM")],
}
REGIONS = list(CITIES_BY_REGION.keys())
VALID_CHANNELS = ["STORE", "ONLINE"]
RETURN_REASON_CODES = [
    "DEFECTIVE", "WRONG_ITEM", "NOT_AS_DESCRIBED",
    "CHANGED_MIND", "SIZE_ISSUE", "LATE_DELIVERY",
]

BASE_PRODUCTS = 500
BASE_STORES = 30
BASE_PROMOTIONS = 50
BASE_ORDERS_PER_DAY = 200
BASE_INVENTORY_SAMPLES_PER_DAY = 800
FULL_RANGE_DAYS = 30
RETURN_RATE = 0.08
SESSION_CONVERSION_RATE = 0.28
CUSTOMER_POOL_SIZE = 4000
DELTA_UPDATE_PCT = 0.05  # fraction of "yesterday" order lines re-sent as corrections


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------

def _fmt_date(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _random_time_on(d: date, rng: random.Random) -> datetime:
    return datetime(d.year, d.month, d.day) + timedelta(
        hours=rng.randint(0, 23), minutes=rng.randint(0, 59), seconds=rng.randint(0, 59)
    )


def write_csv(path: str, fieldnames: list[str], rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def append_raw_lines(path: str, lines: list[str]) -> None:
    """Appends hand-crafted malformed lines directly to a CSV file (bypasses
    the csv writer) to exercise Bronze csv_parse_mode handling."""
    with open(path, "a", newline="", encoding="utf-8") as f:
        for line in lines:
            f.write(line.rstrip("\n") + "\n")


def inject_dirty(rows: list[dict], pct: float, rng: random.Random, corrupt_fn) -> int:
    """Corrupts a random sample of rows in place. Returns count corrupted."""
    if pct <= 0 or not rows:
        return 0
    n = max(1, int(len(rows) * pct))
    idxs = rng.sample(range(len(rows)), min(n, len(rows)))
    for i in idxs:
        corrupt_fn(rows[i])
    return len(idxs)


# --------------------------------------------------------------------------
# Dimension generators (deterministic IDs; always full snapshot)
# --------------------------------------------------------------------------

def generate_products(n: int, rng: random.Random) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        category = rng.choice(list(CATEGORIES.keys()))
        sub_category = rng.choice(CATEGORIES[category])
        cost = round(rng.uniform(2.0, 150.0), 2)
        margin = rng.uniform(1.3, 3.0)
        list_price = round(cost * margin, 2)
        rows.append({
            "product_id": f"PRD{i:05d}",
            "product_name": f"{sub_category} Item {i}",
            "category": category,
            "sub_category": sub_category,
            "brand": rng.choice(BRANDS),
            "cost": cost,
            "list_price": list_price,
            "active_flag": "Y" if rng.random() > 0.05 else "N",
            "created_at": _fmt_date(date(2023, 1, 1) + timedelta(days=rng.randint(0, 700))),
        })
    return rows


def generate_stores(n: int, rng: random.Random) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        region = REGIONS[(i - 1) % len(REGIONS)]
        city, state = rng.choice(CITIES_BY_REGION[region])
        channel = "ONLINE" if i <= max(1, int(n * 0.15)) else "STORE"
        rows.append({
            "store_id": f"STR{i:04d}",
            "store_name": f"{city} {'Fulfillment Center' if channel == 'ONLINE' else 'Store'} {i}",
            "region": region,
            "state": state,
            "city": city,
            "channel": channel,
            "open_date": _fmt_date(date(2018, 1, 1) + timedelta(days=rng.randint(0, 2000))),
        })
    return rows


def generate_promotions(n: int, products: list[dict], as_of: date, rng: random.Random) -> list[dict]:
    rows = []
    range_start = as_of - timedelta(days=FULL_RANGE_DAYS + 15)
    for i in range(1, n + 1):
        product = rng.choice(products)
        start = range_start + timedelta(days=rng.randint(0, FULL_RANGE_DAYS + 10))
        end = start + timedelta(days=rng.randint(3, 21))
        rows.append({
            "promo_id": f"PROMO{i:04d}",
            "promo_name": f"{product['sub_category']} Promo {i}",
            "product_id": product["product_id"],
            "discount_pct": rng.choice([5, 10, 15, 20, 25, 30, 40]),
            "start_date": _fmt_date(start),
            "end_date": _fmt_date(end),
        })
    return rows


def _active_promo_for(product_id: str, on_date: date, promotions: list[dict]) -> dict | None:
    candidates = [
        p for p in promotions
        if p["product_id"] == product_id
        and p["start_date"] <= _fmt_date(on_date) <= p["end_date"]
    ]
    return candidates[0] if candidates else None


# --------------------------------------------------------------------------
# Fact generators
# --------------------------------------------------------------------------

@dataclass
class OrderLineRef:
    order_id: str
    order_line_id: int
    order_ts: datetime
    store_id: str
    product_id: str
    channel: str
    qty: int


def generate_orders_for_day(
    d: date, orders_per_day: int, stores: list[dict], products: list[dict],
    promotions: list[dict], rng: random.Random,
) -> tuple[list[dict], list[OrderLineRef]]:
    rows = []
    refs: list[OrderLineRef] = []
    day_str = d.strftime("%Y%m%d")
    for seq in range(orders_per_day):
        order_id = f"ORD{day_str}{seq:05d}"
        store = rng.choice(stores)
        channel = store["channel"]
        cust_num = rng.randint(1, CUSTOMER_POOL_SIZE)
        customer_id = f"CUST{cust_num:06d}"
        customer_email = f"customer{cust_num}@example.com"
        order_ts = _random_time_on(d, rng)
        n_lines = rng.randint(1, 4)
        for line_id in range(n_lines):
            product = rng.choice(products)
            qty = rng.randint(1, 5)
            unit_price = product["list_price"]
            promo = _active_promo_for(product["product_id"], d, promotions)
            promo_id = promo["promo_id"] if promo else ""
            discount_pct = promo["discount_pct"] if promo else 0
            discount_amt = round(unit_price * qty * discount_pct / 100.0, 2)
            rows.append({
                "order_id": order_id,
                "order_line_id": line_id,
                "order_ts": _fmt_ts(order_ts),
                "store_id": store["store_id"],
                "product_id": product["product_id"],
                "promo_id": promo_id,
                "qty": qty,
                "unit_price": unit_price,
                "discount_amt": discount_amt,
                "channel": channel,
                "customer_id": customer_id,
                "customer_email": customer_email,
            })
            refs.append(OrderLineRef(
                order_id=order_id, order_line_id=line_id, order_ts=order_ts,
                store_id=store["store_id"], product_id=product["product_id"],
                channel=channel, qty=qty,
            ))
    return rows, refs


def generate_order_corrections(prev_day: date, orders_per_day: int, pct: float,
                                 products: list[dict], rng: random.Random) -> list[dict]:
    """Re-emits order_line_id=0 for a subset of 'yesterday's' orders with a
    corrected price/discount, to exercise the Silver UPDATE-on-existing-key path.
    Deterministic order_id scheme means these IDs are always valid without
    needing to read a prior run's output."""
    day_str = prev_day.strftime("%Y%m%d")
    n = max(1, int(orders_per_day * pct))
    rows = []
    for seq in rng.sample(range(orders_per_day), min(n, orders_per_day)):
        order_id = f"ORD{day_str}{seq:05d}"
        product = rng.choice(products)
        qty = rng.randint(1, 5)
        unit_price = product["list_price"]
        discount_amt = round(unit_price * qty * rng.choice([0, 5, 10]) / 100.0, 2)
        rows.append({
            "order_id": order_id,
            "order_line_id": 0,
            "order_ts": _fmt_ts(_random_time_on(prev_day, rng)),
            "store_id": f"STR{rng.randint(1, BASE_STORES):04d}",
            "product_id": product["product_id"],
            "promo_id": "",
            "qty": qty,
            "unit_price": unit_price,
            "discount_amt": discount_amt,
            "channel": rng.choice(VALID_CHANNELS),
            "customer_id": f"CUST{rng.randint(1, CUSTOMER_POOL_SIZE):06d}",
            "customer_email": f"customer{rng.randint(1, CUSTOMER_POOL_SIZE)}@example.com",
        })
    return rows


def generate_returns(order_refs: list[OrderLineRef], rng: random.Random) -> list[dict]:
    rows = []
    seq = 0
    for ref in order_refs:
        if rng.random() > RETURN_RATE:
            continue
        return_ts = ref.order_ts + timedelta(days=rng.randint(1, 14))
        seq += 1
        rows.append({
            "return_id": f"RTN{ref.order_ts:%Y%m%d}{seq:05d}",
            "order_id": ref.order_id,
            "order_line_id": ref.order_line_id,
            "return_ts": _fmt_ts(return_ts),
            "qty": rng.randint(1, ref.qty),
            "reason_code": rng.choice(RETURN_REASON_CODES),
        })
    return rows


def generate_web_sessions(order_refs: list[OrderLineRef], d: date, rng: random.Random) -> list[dict]:
    rows = []
    day_refs = [r for r in order_refs]
    seq = 0
    # Converted sessions tied to real orders
    for ref in day_refs:
        if ref.order_line_id != 0:
            continue  # one session per order, keyed off its first line
        if rng.random() > 0.9:
            continue  # not every order traces back to a captured session
        seq += 1
        session_ts = ref.order_ts - timedelta(minutes=rng.randint(1, 45))
        rows.append({
            "session_id": f"SESS{d:%Y%m%d}{seq:06d}",
            "customer_id": f"CUST{rng.randint(1, CUSTOMER_POOL_SIZE):06d}",
            "session_ts": _fmt_ts(session_ts),
            "store_id": ref.store_id,
            "channel": ref.channel,
            "converted_flag": 1,
            "order_id": ref.order_id,
        })
    # Browse-only, non-converted sessions
    n_orders_today = len({r.order_id for r in day_refs})
    n_browse = int(n_orders_today * (1.0 / SESSION_CONVERSION_RATE - 1.0))
    for _ in range(n_browse):
        seq += 1
        rows.append({
            "session_id": f"SESS{d:%Y%m%d}{seq:06d}",
            "customer_id": f"CUST{rng.randint(1, CUSTOMER_POOL_SIZE):06d}",
            "session_ts": _fmt_ts(_random_time_on(d, rng)),
            "store_id": f"STR{rng.randint(1, BASE_STORES):04d}",
            "channel": rng.choice(VALID_CHANNELS),
            "converted_flag": 0,
            "order_id": "",
        })
    return rows


def generate_inventory_snapshot(d: date, stores: list[dict], products: list[dict],
                                 sample_size: int, rng: random.Random) -> list[dict]:
    rows = []
    seen = set()
    attempts = 0
    while len(rows) < sample_size and attempts < sample_size * 3:
        attempts += 1
        store = rng.choice(stores)
        product = rng.choice(products)
        key = (store["store_id"], product["product_id"])
        if key in seen:
            continue
        seen.add(key)
        stockout = rng.random() < 0.05
        on_hand = 0 if stockout else rng.randint(1, 300)
        rows.append({
            "store_id": store["store_id"],
            "product_id": product["product_id"],
            "snapshot_date": _fmt_date(d),
            "on_hand_qty": on_hand,
            "on_order_qty": rng.randint(0, 100),
        })
    return rows


# --------------------------------------------------------------------------
# Dirty-data corruptors
# --------------------------------------------------------------------------

def corrupt_product(row: dict) -> None:
    choice = random.choice(["null_name", "negative_cost"])
    if choice == "null_name":
        row["product_name"] = ""
    else:
        row["cost"] = -abs(float(row["cost"]))


def corrupt_store(row: dict) -> None:
    choice = random.choice(["null_region", "bad_channel"])
    if choice == "null_region":
        row["region"] = ""
    else:
        row["channel"] = "IN_STORE"  # not a valid enum value


def corrupt_promotion(row: dict) -> None:
    choice = random.choice(["bad_range", "bad_discount"])
    if choice == "bad_range":
        row["start_date"], row["end_date"] = row["end_date"], row["start_date"]
    else:
        row["discount_pct"] = 150


def corrupt_order(row: dict) -> None:
    choice = random.choice(["null_product", "negative_qty", "bad_date", "null_price"])
    if choice == "null_product":
        row["product_id"] = ""
    elif choice == "negative_qty":
        row["qty"] = -row["qty"] if row["qty"] else -1
    elif choice == "bad_date":
        row["order_ts"] = "not-a-date"
    else:
        row["unit_price"] = ""


def corrupt_return(row: dict) -> None:
    choice = random.choice(["orphan_order", "negative_qty"])
    if choice == "orphan_order":
        row["order_id"] = "ORD00000000099999"
    else:
        row["qty"] = -1


def corrupt_session(row: dict) -> None:
    choice = random.choice(["null_ts", "orphan_order"])
    if choice == "null_ts":
        row["session_ts"] = ""
    else:
        row["order_id"] = "ORD00000000099999"


def corrupt_inventory(row: dict) -> None:
    choice = random.choice(["negative_qty", "null_product"])
    if choice == "negative_qty":
        row["on_hand_qty"] = -5
    else:
        row["product_id"] = ""


# --------------------------------------------------------------------------
# Main orchestration
# --------------------------------------------------------------------------

FIELDNAMES = {
    "product": ["product_id", "product_name", "category", "sub_category", "brand",
                "cost", "list_price", "active_flag", "created_at"],
    "store": ["store_id", "store_name", "region", "state", "city", "channel", "open_date"],
    "promotion": ["promo_id", "promo_name", "product_id", "discount_pct", "start_date", "end_date"],
    "orders": ["order_id", "order_line_id", "order_ts", "store_id", "product_id", "promo_id",
               "qty", "unit_price", "discount_amt", "channel", "customer_id", "customer_email"],
    "returns": ["return_id", "order_id", "order_line_id", "return_ts", "qty", "reason_code"],
    "web_sessions": ["session_id", "customer_id", "session_ts", "store_id", "channel",
                      "converted_flag", "order_id"],
    "inventory_snapshot": ["store_id", "product_id", "snapshot_date", "on_hand_qty", "on_order_qty"],
}

MALFORMED_ORDER_LINES = [
    "ORDBADLINE,0,2026-01-01 10:00:00,STR0001,PRD00001",  # too few columns
    'ORDBADLINE2,"unterminated quote,STR0001,PRD00001,,1,9.99,0,STORE,CUST000001,x@example.com',
    "ORDBADLINE3,not_an_int,2026-01-01 10:00:00,STR0001,PRD00001,,1,9.99,0,STORE,CUST000001,x@example.com",
]


def generate(out_dir: str, seed: int, scale: float, as_of_date: date, mode: str,
             dirty_pct: float) -> dict:
    rng = random.Random(seed)
    random.seed(seed)  # corrupt_* helpers use the module-level RNG for simplicity
    os.makedirs(out_dir, exist_ok=True)

    n_products = max(10, int(BASE_PRODUCTS))
    n_stores = max(3, int(BASE_STORES))
    n_promotions = max(5, int(BASE_PROMOTIONS))
    orders_per_day = max(1, int(BASE_ORDERS_PER_DAY * scale))
    inventory_samples = max(1, int(BASE_INVENTORY_SAMPLES_PER_DAY * scale))

    products = generate_products(n_products, rng)
    stores = generate_stores(n_stores, rng)
    promotions = generate_promotions(n_promotions, products, as_of_date, rng)

    stats = {"mode": mode, "as_of_date": _fmt_date(as_of_date)}

    # Dimensions: always full snapshot
    dirty_products = inject_dirty(products, dirty_pct, rng, corrupt_product)
    dirty_stores = inject_dirty(stores, dirty_pct, rng, corrupt_store)
    dirty_promos = inject_dirty(promotions, dirty_pct, rng, corrupt_promotion)
    write_csv(os.path.join(out_dir, "product.csv"), FIELDNAMES["product"], products)
    write_csv(os.path.join(out_dir, "store.csv"), FIELDNAMES["store"], stores)
    write_csv(os.path.join(out_dir, "promotion.csv"), FIELDNAMES["promotion"], promotions)
    stats.update({
        "product_rows": len(products), "product_dirty": dirty_products,
        "store_rows": len(stores), "store_dirty": dirty_stores,
        "promotion_rows": len(promotions), "promotion_dirty": dirty_promos,
    })

    order_rows: list[dict] = []
    all_refs: list[OrderLineRef] = []
    session_rows: list[dict] = []
    inventory_rows: list[dict] = []

    if mode == "full":
        date_range = [as_of_date - timedelta(days=i) for i in range(FULL_RANGE_DAYS - 1, -1, -1)]
        for d in date_range:
            rows, refs = generate_orders_for_day(d, orders_per_day, stores, products, promotions, rng)
            order_rows.extend(rows)
            all_refs.extend(refs)
            session_rows.extend(generate_web_sessions(refs, d, rng))
            inventory_rows.extend(generate_inventory_snapshot(d, stores, products, inventory_samples, rng))
    elif mode == "delta":
        rows, refs = generate_orders_for_day(as_of_date, orders_per_day, stores, products, promotions, rng)
        order_rows.extend(rows)
        all_refs.extend(refs)
        corrections = generate_order_corrections(
            as_of_date - timedelta(days=1), orders_per_day, DELTA_UPDATE_PCT, products, rng
        )
        order_rows.extend(corrections)
        session_rows.extend(generate_web_sessions(refs, as_of_date, rng))
        inventory_rows.extend(generate_inventory_snapshot(as_of_date, stores, products, inventory_samples, rng))
        stats["correction_rows"] = len(corrections)
    else:
        raise ValueError(f"Unknown mode: {mode}")

    return_rows = generate_returns(all_refs, rng)

    dirty_orders = inject_dirty(order_rows, dirty_pct, rng, corrupt_order)
    dirty_returns = inject_dirty(return_rows, dirty_pct, rng, corrupt_return)
    dirty_sessions = inject_dirty(session_rows, dirty_pct, rng, corrupt_session)
    dirty_inventory = inject_dirty(inventory_rows, dirty_pct, rng, corrupt_inventory)

    orders_path = os.path.join(out_dir, "orders.csv")
    write_csv(orders_path, FIELDNAMES["orders"], order_rows)
    append_raw_lines(orders_path, MALFORMED_ORDER_LINES)
    write_csv(os.path.join(out_dir, "returns.csv"), FIELDNAMES["returns"], return_rows)
    write_csv(os.path.join(out_dir, "web_sessions.csv"), FIELDNAMES["web_sessions"], session_rows)
    write_csv(os.path.join(out_dir, "inventory_snapshot.csv"), FIELDNAMES["inventory_snapshot"], inventory_rows)

    stats.update({
        "orders_rows": len(order_rows), "orders_dirty": dirty_orders,
        "orders_malformed_raw_lines": len(MALFORMED_ORDER_LINES),
        "returns_rows": len(return_rows), "returns_dirty": dirty_returns,
        "web_sessions_rows": len(session_rows), "web_sessions_dirty": dirty_sessions,
        "inventory_snapshot_rows": len(inventory_rows), "inventory_snapshot_dirty": dirty_inventory,
    })
    return stats


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out-dir", default="data/mock", help="Output directory for generated CSVs")
    p.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    p.add_argument("--scale", type=float, default=1.0, help="Scale factor for fact table volumes")
    p.add_argument("--as-of-date", default=_fmt_date(date.today()),
                    help="Reference date (YYYY-MM-DD); full mode uses the 30 days up to this date, "
                         "delta mode uses this single date")
    p.add_argument("--mode", choices=["full", "delta"], default="full")
    p.add_argument("--dirty-pct", type=float, default=0.02,
                    help="Fraction of rows per file to deliberately corrupt (0 disables)")
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    as_of = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()
    stats = generate(args.out_dir, args.seed, args.scale, as_of, args.mode, args.dirty_pct)
    print(f"Generated mock data in {args.out_dir} (mode={args.mode}, seed={args.seed}, scale={args.scale})")
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
