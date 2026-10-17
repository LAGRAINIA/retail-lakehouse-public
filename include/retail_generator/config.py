"""
Static configuration of the simulated company: "Stride Sports", an omnichannel sports retailer
(stores in 5 countries + an e-commerce site).

Everything is deterministic: the same date always produces the same files, which makes Airflow
re-runs and backfills idempotent.
"""

from __future__ import annotations

import os
from datetime import date, timedelta

# First day of simulated history. Override with RETAIL_EPOCH=YYYY-MM-DD.
EPOCH = date.fromisoformat(os.getenv("RETAIL_EPOCH", "2026-07-01"))

# Business events, as offsets from EPOCH so they always fall inside the generated history.
PRICE_INCREASE_DATE = EPOCH + timedelta(days=30)    # +5% on 20% of the catalogue (SCD2 changes)
NEW_STORE_OPENING = EPOCH + timedelta(days=45)      # ST012 Toulouse opens (announced 14 days before)
POS_UPGRADE_DATE = EPOCH + timedelta(days=60)       # POS v2: new column + ISO timestamps with offset
WEB_DEVICE_FIELD_DATE = EPOCH + timedelta(days=75)  # e-commerce JSON gains a "device" field

INITIAL_CUSTOMERS = 6000

# store_id, name, city, country, timezone, currency, size factor, opening date
STORES = [
    ("ST001", "Paris La Defense", "Paris", "FR", "Europe/Paris", "EUR", 2.5, "2014-03-01"),  # flagship: skew
    ("ST002", "Lyon Part-Dieu", "Lyon", "FR", "Europe/Paris", "EUR", 1.2, "2016-09-15"),
    ("ST003", "Lille Centre", "Lille", "FR", "Europe/Paris", "EUR", 1.0, "2017-04-01"),
    ("ST004", "Marseille Prado", "Marseille", "FR", "Europe/Paris", "EUR", 1.0, "2018-02-10"),
    ("ST005", "Bordeaux Lac", "Bordeaux", "FR", "Europe/Paris", "EUR", 0.9, "2019-06-01"),
    ("ST006", "Madrid Castellana", "Madrid", "ES", "Europe/Madrid", "EUR", 1.1, "2019-10-01"),
    ("ST007", "Barcelona Diagonal", "Barcelona", "ES", "Europe/Madrid", "EUR", 1.0, "2020-03-01"),
    ("ST008", "Milano Duomo", "Milan", "IT", "Europe/Rome", "EUR", 1.0, "2021-05-20"),
    ("ST009", "London Oxford Street", "London", "GB", "Europe/London", "GBP", 1.3, "2021-11-01"),
    ("ST010", "Manchester Arndale", "Manchester", "GB", "Europe/London", "GBP", 0.8, "2022-08-01"),
    ("ST011", "Geneve Balexert", "Geneva", "CH", "Europe/Zurich", "CHF", 0.7, "2023-04-01"),
    ("ST012", "Toulouse Blagnac", "Toulouse", "FR", "Europe/Paris", "EUR", 0.9, NEW_STORE_OPENING.isoformat()),
]

COUNTRY_CURRENCY = {"FR": "EUR", "ES": "EUR", "IT": "EUR", "BE": "EUR", "GB": "GBP", "CH": "CHF"}

# Reference FX rates (1 EUR = x). A deterministic drift is applied per day.
FX_BASE = {"GBP": 0.855, "CHF": 0.938, "USD": 1.085}

# sport -> (house brand, {category: (min price, max price)}, monthly seasonality Jan..Dec)
SPORTS = {
    "running": ("Runova", {"shoes": (40, 160), "t-shirts": (8, 35), "shorts": (10, 30), "watches": (60, 300),
                           "accessories": (5, 25)},
                [1.2, 1.0, 1.1, 1.2, 1.2, 1.1, 1.0, 0.9, 1.2, 1.1, 0.9, 0.8]),
    "cycling": ("Pedalix", {"bikes": (250, 1500), "helmets": (20, 90), "jerseys": (20, 60), "lights": (10, 45),
                            "tools": (5, 40)},
                [0.5, 0.6, 0.9, 1.3, 1.5, 1.5, 1.4, 1.2, 1.0, 0.8, 0.6, 0.8]),
    "hiking": ("Summitra", {"boots": (50, 180), "backpacks": (20, 140), "jackets": (30, 200), "tents": (40, 400),
                            "poles": (15, 60)},
               [0.6, 0.6, 0.8, 1.1, 1.3, 1.5, 1.6, 1.5, 1.1, 0.9, 0.7, 0.8]),
    "swimming": ("Aquaflow", {"swimsuits": (10, 45), "goggles": (5, 30), "caps": (3, 12), "towels": (10, 30)},
                 [0.5, 0.5, 0.6, 0.8, 1.2, 1.8, 2.0, 1.8, 0.9, 0.6, 0.5, 0.5]),
    "football": ("Goalix", {"balls": (8, 40), "boots": (25, 130), "jerseys": (15, 70), "shin-guards": (5, 25)},
                 [0.9, 0.9, 1.0, 1.0, 1.0, 1.1, 0.8, 1.3, 1.4, 1.0, 0.9, 1.0]),
    "fitness": ("Fitora", {"dumbbells": (10, 120), "mats": (10, 50), "bands": (5, 25), "apparel": (10, 40)},
                [1.8, 1.4, 1.1, 1.0, 1.0, 0.9, 0.8, 0.8, 1.1, 1.0, 1.0, 1.1]),
    "ski": ("Skivo", {"skis": (150, 600), "ski-boots": (100, 400), "ski-jackets": (60, 250), "gloves": (10, 50),
                      "ski-goggles": (20, 120)},
            [2.0, 1.8, 0.9, 0.2, 0.05, 0.02, 0.02, 0.05, 0.2, 0.8, 1.6, 2.2]),
}

PRODUCTS_PER_CATEGORY = 9
UNKNOWN_SKU = "SKU999999"

FIRST_NAMES = {
    "FR": ["Lucas", "Emma", "Hugo", "Lea", "Louis", "Chloe", "Nathan", "Manon", "Jules", "Camille"],
    "ES": ["Pablo", "Lucia", "Hugo", "Sofia", "Mateo", "Martina", "Daniel", "Paula"],
    "IT": ["Marco", "Giulia", "Luca", "Sofia", "Matteo", "Chiara", "Andrea", "Sara"],
    "GB": ["Oliver", "Amelia", "George", "Isla", "Harry", "Ava", "Jack", "Mia"],
    "CH": ["Noah", "Mia", "Liam", "Emma", "Luca", "Lina", "Leon", "Elena"],
    "BE": ["Arthur", "Olivia", "Louis", "Elena", "Noah", "Lina"],
}
LAST_NAMES = {
    "FR": ["Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand", "Leroy", "Moreau"],
    "ES": ["Garcia", "Rodriguez", "Gonzalez", "Fernandez", "Lopez", "Martinez", "Sanchez"],
    "IT": ["Rossi", "Russo", "Ferrari", "Esposito", "Bianchi", "Romano", "Colombo"],
    "GB": ["Smith", "Jones", "Taylor", "Brown", "Williams", "Wilson", "Johnson"],
    "CH": ["Muller", "Meier", "Schmid", "Keller", "Weber", "Huber", "Favre"],
    "BE": ["Peeters", "Janssens", "Maes", "Jacobs", "Mertens", "Dubois"],
}
CITIES = {
    "FR": [("Paris", "75011"), ("Lyon", "69003"), ("Lille", "59000"), ("Marseille", "13008"),
           ("Bordeaux", "33000"), ("Toulouse", "31000"), ("Nantes", "44000")],
    "ES": [("Madrid", "28046"), ("Barcelona", "08036"), ("Valencia", "46001")],
    "IT": [("Milano", "20121"), ("Roma", "00184"), ("Torino", "10121")],
    "GB": [("London", "W1D 1BS"), ("Manchester", "M4 3AQ"), ("Leeds", "LS1 4AP")],
    "CH": [("Geneve", "1203"), ("Lausanne", "1003"), ("Zurich", "8001")],
    "BE": [("Bruxelles", "1000"), ("Liege", "4000")],
}
CUSTOMER_COUNTRY_WEIGHTS = {"FR": 0.48, "ES": 0.14, "IT": 0.10, "GB": 0.14, "CH": 0.06, "BE": 0.08}

RETURN_REASONS = ["wrong_size", "defective", "changed_mind", "not_as_described", "late_delivery"]

# Named incidents that can be injected on one landing date (see docs/SCENARIOS.md).
INCIDENTS = {
    "price_x100": "ERP export bug: every product price is multiplied by 100",
    "empty_pos_file": "ST003 sends a POS file with a header and no rows",
    "duplicate_batch": "the whole POS batch of the day is sent twice",
    "guest_checkout_flood": "a checkout bug drops customer_id on 80% of web orders (join skew)",
    "pos_outage": "the POS integration is down: no store file lands today (fix: re-run the day)",
    "schema_break": "the POS export renames 'quantity' to 'qty'",
}
