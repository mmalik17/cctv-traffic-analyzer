"""
Jakarta Traffic Analysis — Read Pipeline
Query Supabase data with flexible filters via direct PostgreSQL.
"""

import os
import psycopg2
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

WIB = timezone(timedelta(hours=7))

DB_HOST     = os.environ["DB_HOST"]
DB_PORT     = os.environ.get("DB_PORT", "5432")
DB_NAME     = os.environ.get("DB_NAME", "postgres")
DB_USER     = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ["DB_PASSWORD"]

def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD, sslmode="require"
    )

def query_rows(filter_mode="all", date=None, range_from=None,
               range_to=None, location=None, limit=500) -> list:
    today     = datetime.now(WIB).date()
    yesterday = today - timedelta(days=1)

    conditions, params = [], []

    if filter_mode == "today":
        conditions.append("timestamp >= %s AND timestamp <= %s")
        params += [f"{today} 00:00:00", f"{today} 23:59:59"]
    elif filter_mode == "yesterday":
        conditions.append("timestamp >= %s AND timestamp <= %s")
        params += [f"{yesterday} 00:00:00", f"{yesterday} 23:59:59"]
    elif filter_mode == "date" and date:
        conditions.append("timestamp >= %s AND timestamp <= %s")
        params += [f"{date} 00:00:00", f"{date} 23:59:59"]
    elif filter_mode == "range" and range_from and range_to:
        conditions.append("timestamp >= %s AND timestamp <= %s")
        params += [range_from, range_to]

    if location:
        conditions.append("location = %s")
        params.append(location)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql   = f"SELECT * FROM cctv_traffic {where} ORDER BY timestamp ASC LIMIT %s"
    params.append(limit)

    conn = get_db_conn()
    cur  = conn.cursor()
    cur.execute(sql, params)
    cols = [desc[0] for desc in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows

def main():
    # ── Edit these as needed ──────────────────────────
    rows = query_rows(
        filter_mode="today",    # "all"|"today"|"yesterday"|"date"|"range"
        # date="2026-06-12",
        # range_from="2026-06-12 07:00:00",
        # range_to="2026-06-12 09:00:00",
        location=None,
        limit=500,
    )

    print(f"✅ {len(rows)} rows found\n")

    for r in rows:
        jam_icon = "🔴" if r.get("traffic_jam_label") else "🟢"
        print(
            f"{jam_icon} {r['timestamp']} | {str(r['location']):30s} | "
            f"density={float(r.get('density') or 0)*100:5.1f}% | "
            f"vehicles={r.get('vehicle_count') or 0:2d} "
            f"(car={r.get('car_count')}, moto={r.get('motorcycle_count')}, "
            f"bus={r.get('bus_count')}, truck={r.get('truck_count')})"
        )

if __name__ == "__main__":
    main()