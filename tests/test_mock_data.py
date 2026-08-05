"""Local sanity tests for jobs/generate_mock_data.py.

Run with: pytest tests/test_mock_data.py -v

These tests don't touch AWS — they validate that the generator produces
well-formed reference data, plausible referential integrity on the clean
majority of rows, and that the deliberately-injected dirty rows land within
their expected bounds (used later to prove Silver's quarantine logic catches
exactly what we expect).
"""
import csv
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))

import generate_mock_data as gen  # noqa: E402

SCALE = 0.2
SEED = 7
DIRTY_PCT = 0.05


def _read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_full_mode_generates_all_files(tmp_path):
    out_dir = str(tmp_path / "full")
    stats = gen.generate(out_dir, SEED, SCALE, date(2026, 8, 3), "full", DIRTY_PCT)

    expected_files = [
        "product.csv", "store.csv", "promotion.csv", "orders.csv",
        "returns.csv", "web_sessions.csv", "inventory_snapshot.csv",
    ]
    for fname in expected_files:
        path = os.path.join(out_dir, fname)
        assert os.path.exists(path), f"missing {fname}"
        assert os.path.getsize(path) > 0

    assert stats["product_rows"] == gen.BASE_PRODUCTS
    assert stats["store_rows"] == gen.BASE_STORES
    assert stats["promotion_rows"] == gen.BASE_PROMOTIONS
    assert stats["orders_rows"] > 0
    assert stats["returns_rows"] > 0
    assert stats["web_sessions_rows"] > 0
    assert stats["inventory_snapshot_rows"] > 0


def test_dimension_business_keys_unique(tmp_path):
    out_dir = str(tmp_path / "full")
    gen.generate(out_dir, SEED, SCALE, date(2026, 8, 3), "full", 0.0)  # dirty_pct=0: keys must be clean-unique

    products = _read_csv(os.path.join(out_dir, "product.csv"))
    stores = _read_csv(os.path.join(out_dir, "store.csv"))
    promotions = _read_csv(os.path.join(out_dir, "promotion.csv"))

    assert len({r["product_id"] for r in products}) == len(products)
    assert len({r["store_id"] for r in stores}) == len(stores)
    assert len({r["promo_id"] for r in promotions}) == len(promotions)


def test_orders_referential_integrity_mostly_holds(tmp_path):
    out_dir = str(tmp_path / "full")
    stats = gen.generate(out_dir, SEED, SCALE, date(2026, 8, 3), "full", DIRTY_PCT)

    products = _read_csv(os.path.join(out_dir, "product.csv"))
    stores = _read_csv(os.path.join(out_dir, "store.csv"))
    product_ids = {r["product_id"] for r in products}
    store_ids = {r["store_id"] for r in stores}

    orders = _read_csv(os.path.join(out_dir, "orders.csv"))
    # malformed raw lines parse into short/garbled DictReader rows; drop rows
    # that don't even have a product_id key populated by the header mapping
    parsed_orders = [r for r in orders if r.get("order_id", "").startswith("ORD2026")]

    orphan_products = sum(1 for r in parsed_orders if r["product_id"] and r["product_id"] not in product_ids)
    orphan_stores = sum(1 for r in parsed_orders if r["store_id"] and r["store_id"] not in store_ids)

    # only the deliberately-injected dirty rows should violate RI
    assert orphan_products == 0, "product_id FK should always resolve for real product ids"
    assert orphan_stores == 0, "store_id FK should always resolve for real store ids"

    # roughly dirty_pct of orders rows should have a null product_id (one of
    # several corruption types selected uniformly at random)
    null_product_rows = sum(1 for r in parsed_orders if r["product_id"] == "")
    assert null_product_rows <= int(stats["orders_dirty"])


def test_orders_csv_has_malformed_lines_for_parse_mode_testing(tmp_path):
    out_dir = str(tmp_path / "full")
    gen.generate(out_dir, SEED, SCALE, date(2026, 8, 3), "full", DIRTY_PCT)

    with open(os.path.join(out_dir, "orders.csv"), encoding="utf-8") as f:
        content = f.read()

    for line in gen.MALFORMED_ORDER_LINES:
        assert line.split(",")[0] in content


def test_returns_reference_valid_order_lines_mostly(tmp_path):
    out_dir = str(tmp_path / "full")
    gen.generate(out_dir, SEED, SCALE, date(2026, 8, 3), "full", 0.0)

    orders = _read_csv(os.path.join(out_dir, "orders.csv"))
    order_keys = {(r["order_id"], r["order_line_id"]) for r in orders if r["order_id"].startswith("ORD2026")}

    returns = _read_csv(os.path.join(out_dir, "returns.csv"))
    orphans = sum(1 for r in returns if (r["order_id"], r["order_line_id"]) not in order_keys)
    assert orphans == 0, "with dirty_pct=0, every return should trace back to a real order line"


def test_delta_mode_is_smaller_and_includes_corrections(tmp_path):
    full_dir = str(tmp_path / "full")
    delta_dir = str(tmp_path / "delta")
    full_stats = gen.generate(full_dir, SEED, SCALE, date(2026, 8, 3), "full", 0.0)
    delta_stats = gen.generate(delta_dir, SEED, SCALE, date(2026, 8, 4), "delta", 0.0)

    assert delta_stats["orders_rows"] < full_stats["orders_rows"]
    assert delta_stats["correction_rows"] > 0

    delta_orders = _read_csv(os.path.join(delta_dir, "orders.csv"))
    correction_day = date(2026, 8, 3).strftime("%Y%m%d")
    corrections = [
        r for r in delta_orders
        if r["order_id"].startswith(f"ORD{correction_day}") and r["order_line_id"] == "0"
    ]
    assert len(corrections) == delta_stats["correction_rows"]

    # every correction's order_id must be a valid id shape that a full run for
    # the prior day would also have produced (same deterministic ID scheme)
    for row in corrections:
        seq = int(row["order_id"][-5:])
        assert 0 <= seq < int(gen.BASE_ORDERS_PER_DAY * SCALE)


def test_dimension_dirty_rows_within_expected_bounds(tmp_path):
    out_dir = str(tmp_path / "full")
    stats = gen.generate(out_dir, SEED, SCALE, date(2026, 8, 3), "full", DIRTY_PCT)

    expected_product_dirty = max(1, int(stats["product_rows"] * DIRTY_PCT))
    expected_store_dirty = max(1, int(stats["store_rows"] * DIRTY_PCT))

    assert stats["product_dirty"] == expected_product_dirty
    assert stats["store_dirty"] == expected_store_dirty


def test_deterministic_with_same_seed(tmp_path):
    dir_a = str(tmp_path / "a")
    dir_b = str(tmp_path / "b")
    stats_a = gen.generate(dir_a, SEED, SCALE, date(2026, 8, 3), "full", DIRTY_PCT)
    stats_b = gen.generate(dir_b, SEED, SCALE, date(2026, 8, 3), "full", DIRTY_PCT)

    assert stats_a["orders_rows"] == stats_b["orders_rows"]
    with open(os.path.join(dir_a, "product.csv")) as fa, open(os.path.join(dir_b, "product.csv")) as fb:
        assert fa.read() == fb.read()
