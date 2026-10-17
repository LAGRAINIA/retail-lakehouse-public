"""The generator must be deterministic (idempotent re-runs) and produce the documented defects."""

import csv
import filecmp
import json
from datetime import timedelta

from include.retail_generator import config as C
from include.retail_generator.generate import generate_batch


def test_same_day_gives_identical_files(tmp_path):
    a = generate_batch(C.EPOCH, tmp_path / "a")
    b = generate_batch(C.EPOCH, tmp_path / "b")
    assert a == b
    for rel in a:
        assert filecmp.cmp(tmp_path / "a" / rel, tmp_path / "b" / rel, shallow=False)


def test_pos_schema_drift_after_upgrade(tmp_path):
    files = generate_batch(C.POS_UPGRADE_DATE, tmp_path)
    pos = next(f for f in files if f.startswith("pos/sales/"))
    with open(tmp_path / pos) as f:
        row = next(csv.DictReader(f))
    assert "loyalty_points" in row
    assert row["transaction_ts"][-6] in "+-"            # ISO 8601 with offset


def test_price_incident_multiplies_prices(tmp_path):
    normal = generate_batch(C.EPOCH, tmp_path / "n")
    broken = generate_batch(C.EPOCH, tmp_path / "b", incidents=("price_x100",))
    path = next(f for f in normal if f.startswith("erp/products/"))
    with open(tmp_path / "n" / path) as f1, open(tmp_path / "b" / path) as f2:
        p1, p2 = next(csv.DictReader(f1)), next(csv.DictReader(f2))
    assert float(p2["unit_price_eur"]) == float(p1["unit_price_eur"]) * 100


def test_fx_rates_only_on_business_days(tmp_path):
    saturday = C.EPOCH + timedelta(days=(5 - C.EPOCH.weekday()) % 7)
    assert not any(f.startswith("finance/") for f in generate_batch(saturday, tmp_path))


def test_web_orders_are_nested_json(tmp_path):
    files = generate_batch(C.EPOCH + timedelta(days=3), tmp_path)
    path = next(f for f in files if f.startswith("ecom/orders/"))
    first = json.loads(open(tmp_path / path).readline())
    assert first["lines"] and {"line_id", "sku", "quantity", "unit_price"} <= set(first["lines"][0])
