import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "app" / "local_database.db"
COMPLAINT_SHARD_COUNT = 3


def complaint_shard_ids():
    return list(range(COMPLAINT_SHARD_COUNT))


def complaint_shard_table(shard_id):
    return f"shard_{shard_id}_complaint"


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    print("Assignment 4 - Sharding Verification")
    print(f"Database path: {DB_PATH}")
    print()

    base_total = conn.execute(
        "SELECT COUNT(*) AS total FROM complaint"
    ).fetchone()["total"]
    map_total = conn.execute(
        "SELECT COUNT(*) AS total FROM complaint_shard_map"
    ).fetchone()["total"]

    shard_total = 0
    for shard_id in complaint_shard_ids():
        table_name = complaint_shard_table(shard_id)
        row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS total,
                COUNT(DISTINCT member_id) AS members,
                MIN(complaint_id) AS min_id,
                MAX(complaint_id) AS max_id
            FROM {table_name}
            """
        ).fetchone()
        shard_total += row["total"]

        print(
            f"Shard {shard_id}: table={table_name}, "
            f"complaints={row['total']}, "
            f"distinct_members={row['members']}, "
            f"id_range={row['min_id']}..{row['max_id']}"
        )

    print()
    print(f"Base complaint count: {base_total}")
    print(f"Shard map count:      {map_total}")
    print(f"Shard total count:    {shard_total}")
    print()

    if base_total == map_total == shard_total:
        print("Verification result: counts match across base table, map, and shards.")
    else:
        print("Verification result: count mismatch detected.")

    duplicate_total = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM (
            SELECT complaint_id
            FROM (
                SELECT complaint_id FROM shard_0_complaint
                UNION ALL
                SELECT complaint_id FROM shard_1_complaint
                UNION ALL
                SELECT complaint_id FROM shard_2_complaint
            )
            GROUP BY complaint_id
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()["total"]

    wrong_shard_total = 0
    for shard_id in complaint_shard_ids():
        table_name = complaint_shard_table(shard_id)
        wrong_shard_total += conn.execute(
            f"SELECT COUNT(*) AS total FROM {table_name} WHERE member_id % 3 <> ?",
            (shard_id,)
        ).fetchone()["total"]

    print(f"Duplicate records across shards: {duplicate_total}")
    print(f"Rows in wrong shard:            {wrong_shard_total}")

    sample_rows = conn.execute(
        """
        SELECT complaint_id, member_id, shard_id
        FROM complaint_shard_map
        ORDER BY complaint_id DESC
        LIMIT 10
        """
    ).fetchall()

    print()
    print("Recent routing samples:")
    for row in sample_rows:
        print(
            f"complaint_id={row['complaint_id']}, "
            f"member_id={row['member_id']}, "
            f"shard_id={row['shard_id']}"
        )

    conn.close()


if __name__ == "__main__":
    main()
