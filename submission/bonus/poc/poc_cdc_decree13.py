"""PoC: CDC Stream Ingestion with Decree 13 Compliance & Late-Arriving Event MERGE.

Demonstrates:
1. In-line PII Tokenization (SHA-256 + Salt) at Bronze landing.
2. Late-arriving event resolution via conditional Delta MERGE (`src.event_ts > tgt.event_ts`).
3. Right-to-erasure workflow with Delta Table deletion & Time Travel audit.
"""
from __future__ import annotations

import datetime as dtm
import hashlib
import time
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

POC_DIR = Path(__file__).resolve().parent / "_lakehouse_poc"
BRONZE = str(POC_DIR / "bronze_trips")
SILVER = str(POC_DIR / "silver_trips")

SALT = "VN_DECREE13_SECRET_SALT_2026"


def tokenize_pii(phone: str, national_id: str) -> tuple[str, str]:
    """Deterministic pseudonymization using salted SHA-256."""
    phone_token = hashlib.sha256(f"{phone}:{SALT}".encode()).hexdigest()[:16]
    id_token = hashlib.sha256(f"{national_id}:{SALT}".encode()).hexdigest()[:16]
    return f"token_p_{phone_token}", f"token_id_{id_token}"


def test_poc_flow():
    print("=" * 70)
    print("PoC: CDC Ride-Hailing Lakehouse with Decree 13 Compliance")
    print("=" * 70)

    # 1. Step 1: Simulate Ingestion into Bronze with Tokenization
    raw_trips = [
        {"trip_id": "T001", "driver_phone": "0912345678", "driver_id_card": "001200000001", "event_ts": dtm.datetime(2026, 8, 18, 8, 0, 0), "status": "REQUESTED", "fare_vnd": 50000},
        {"trip_id": "T002", "driver_phone": "0987654321", "driver_id_card": "001200000002", "event_ts": dtm.datetime(2026, 8, 18, 8, 5, 0), "status": "COMPLETED", "fare_vnd": 120000},
    ]

    bronze_data = []
    for r in raw_trips:
        p_tok, id_tok = tokenize_pii(r["driver_phone"], r["driver_id_card"])
        bronze_data.append({
            "trip_id": r["trip_id"],
            "driver_phone_token": p_tok,
            "driver_id_token": id_tok,
            "event_ts": r["event_ts"],
            "status": r["status"],
            "fare_vnd": r["fare_vnd"],
            "ingest_ts": dtm.datetime.now(),
        })

    bronze_tbl = pa.Table.from_pylist(bronze_data)
    write_deltalake(BRONZE, bronze_tbl, mode="overwrite")
    print(f"\n[1] Bronze Ingested: {DeltaTable(BRONZE).count()} records with PII tokenized.")

    # 2. Step 2: Initial Sync to Silver
    write_deltalake(SILVER, bronze_tbl, mode="overwrite")
    dt_silver = DeltaTable(SILVER)
    print(f"[2] Silver Initialized at version v{dt_silver.version()}: {dt_silver.count()} records.")

    # 3. Step 3: Handle Late-Arriving Out-of-Order CDC Update
    # Case: A newer event arrives (T001 -> COMPLETED at 08:30)
    # Along with a stale late event (T001 -> ACCEPTED at 08:10 coming LATE)
    late_events = [
        {"trip_id": "T001", "driver_phone_token": bronze_data[0]["driver_phone_token"], "driver_id_token": bronze_data[0]["driver_id_token"], "event_ts": dtm.datetime(2026, 8, 18, 8, 30, 0), "status": "COMPLETED", "fare_vnd": 55000, "ingest_ts": dtm.datetime.now()},
        {"trip_id": "T001", "driver_phone_token": bronze_data[0]["driver_phone_token"], "driver_id_token": bronze_data[0]["driver_id_token"], "event_ts": dtm.datetime(2026, 8, 18, 8, 10, 0), "status": "ACCEPTED", "fare_vnd": 50000, "ingest_ts": dtm.datetime.now()},
    ]

    # Perform Stateful MERGE (only update if incoming event_ts > existing event_ts)
    for evt in late_events:
        evt_tbl = pa.Table.from_pylist([evt])
        (
            dt_silver.merge(
                source=evt_tbl,
                predicate="target.trip_id = source.trip_id",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all(predicate="source.event_ts > target.event_ts")
            .when_not_matched_insert_all()
            .execute()
        )

    dt_silver = DeltaTable(SILVER)
    result = dt_silver.to_pyarrow_table().to_pylist()
    t001 = next(r for r in result if r["trip_id"] == "T001")
    print(f"\n[3] Late-Data MERGE Result for T001: status={t001['status']} (Expected: COMPLETED, not ACCEPTED)")
    assert t001["status"] == "COMPLETED", "Late out-of-order event erroneously overwrote newer state!"

    # 4. Step 4: Right-to-Erasure Workflow (Decree 13 Article 16)
    target_token = bronze_data[0]["driver_phone_token"]
    print(f"\n[4] Executing Right-to-Erasure for driver {target_token}...")
    dt_silver.delete(f"driver_phone_token = '{target_token}'")
    
    dt_after_delete = DeltaTable(SILVER)
    remaining_rows = dt_after_delete.to_pyarrow_table().to_pylist()
    print(f"  Rows remaining after erasure: {len(remaining_rows)} (Version {dt_after_delete.version()})")
    assert not any(r["driver_phone_token"] == target_token for r in remaining_rows)
    print("  [PASS] Driver PII token completely removed from active table.")

    # Cleanup temporary PoC lakehouse dir
    import shutil
    shutil.rmtree(POC_DIR, ignore_errors=True)

    print("\n" + "=" * 70)
    print("All PoC assertions PASSED successfully!")
    print("=" * 70)


if __name__ == "__main__":
    test_poc_flow()
