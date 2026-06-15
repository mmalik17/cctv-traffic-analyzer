"""
Jakarta Traffic Analysis — Scrape Pipeline
Scrapes CCTV images, runs YOLO analysis, uploads to Supabase.
Run on schedule via GitHub Actions.
"""

import asyncio
import os
import time
import hashlib
import psycopg2
import requests
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from playwright.async_api import async_playwright
from ultralytics import YOLO

# ── Load Environment Variables ──────────────────────────
load_dotenv()

# ── Config ──────────────────────────────────────────────
WIB = timezone(timedelta(hours=7))

# Supabase Storage (REST API)
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BUCKET_NAME  = os.environ["BUCKET_NAME"]

# Supabase Database (direct PostgreSQL)
DB_HOST     = os.environ["DB_HOST"]
DB_PORT     = os.environ.get("DB_PORT", "5432")
DB_NAME     = os.environ.get("DB_NAME", "postgres")
DB_USER     = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ["DB_PASSWORD"]

# ── CCTV Mapping: url → human-readable location name ───
CCTV_MAPPING = {
    "https://cctv.balitower.co.id/Gelora-017-700470_2/embed.html":"GBK_Senayan_Park",
    "https://cctv.balitower.co.id/Gelora-004-700047_2/embed.html":"GBK_Sudirman",
    "https://cctv.balitower.co.id/Gelora-005-700048_3/embed.html":"GBK_Asia_Afrika",
    "https://streaming-cct.co.id/LiveApp/play.html?name=258587986295618618480038": "Cawang_KM_0",
    "https://streaming-cct.co.id/LiveApp/play.html?name=716294693568887939935263": "JORR_KM_49",
    "https://cctv.balitower.co.id/Bendungan-Hilir-003-700014_1/embed.html": "Depan_Gedung_DPR",
    "https://cctv.balitower.co.id/Karet-Tengsin-004-700085_4/embed.html": "Karet",
    "https://cctv.balitower.co.id/Kuningan-Barat-003-705052_2/embed.html": "Kuningan_Barat"
    # add more url: location pairs here
}

VEHICLE_CLASSES   = {1: "bicycle", 2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}
MOTORIZED_4W      = {"car", "bus", "truck"}
DENSITY_THRESHOLD = 0.20

OUTPUT_DIR = "cctv_captures"
os.makedirs(OUTPUT_DIR, exist_ok=True)

ERROR_PATTERNS = ["Could not play video", "Code:404", "Playback error"]


# ── TIMING SETUP ─────────────────────────────────────────
TIMING = {}

def start_timer():
    return time.time()

def log_timing(step_name, t_start):
    elapsed = time.time() - t_start
    TIMING[step_name] = TIMING.get(step_name, 0) + elapsed
    print(f"⏱️  {step_name}: {elapsed:.2f}s")


# ── Database helpers (psycopg2) ───────────────────────────
def get_db_conn():
    """Open a direct PostgreSQL connection to Supabase."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode="require"
    )

def insert_full_row(record: dict) -> None:
    """Upsert one row into cctv_traffic via direct PostgreSQL."""
    sql = """
        INSERT INTO cctv_traffic (
            timestamp, location, web_url, image_url, annotated_image_url,
            traffic_jam_label, vehicle_count, density,
            car_count, motorcycle_count, bus_count, truck_count, bicycle_count
        ) VALUES (
            %(timestamp)s, %(location)s, %(web_url)s, %(image_url)s, %(annotated_image_url)s,
            %(traffic_jam_label)s, %(vehicle_count)s, %(density)s,
            %(car_count)s, %(motorcycle_count)s, %(bus_count)s, %(truck_count)s, %(bicycle_count)s
        )
        ON CONFLICT (image_url) DO UPDATE SET
            traffic_jam_label   = EXCLUDED.traffic_jam_label,
            vehicle_count       = EXCLUDED.car_count + EXCLUDED.bus_count + EXCLUDED.truck_count,
            density             = EXCLUDED.density,
            annotated_image_url = EXCLUDED.annotated_image_url,
            car_count           = EXCLUDED.car_count,
            motorcycle_count    = EXCLUDED.motorcycle_count,
            bus_count           = EXCLUDED.bus_count,
            truck_count         = EXCLUDED.truck_count,
            bicycle_count       = EXCLUDED.bicycle_count
    """
    try:
        conn = get_db_conn()
        cur  = conn.cursor()
        cur.execute(sql, record)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Database insert failed: {e}")


def query_rows(filter_mode="all", date=None, range_from=None,
               range_to=None, location=None, limit=500) -> list:
    """Query cctv_traffic with flexible filters."""
    today     = datetime.now(WIB).date()
    yesterday = today - timedelta(days=1)

    conditions = []
    params     = []

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

    try:
        conn = get_db_conn()
        cur  = conn.cursor()
        cur.execute(sql, params)
        cols = [desc[0] for desc in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return []


# ── Storage helpers (REST API via requests) ──────────────
def upload_image(filename: str) -> str | None:
    """Upload image directly to Supabase Storage via REST API."""
    if not filename or not os.path.exists(filename):
        return None

    storage_path = os.path.basename(filename)
    upload_url   = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_NAME}/{storage_path}"

    headers = {
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "image/png",
        "x-upsert":      "true",
    }

    try:
        with open(filename, "rb") as f:
            response = requests.put(upload_url, headers=headers, data=f, timeout=30)
        if response.status_code in (200, 201):
            return f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET_NAME}/{storage_path}"
        else:
            print(f"❌ Upload failed ({response.status_code}): {response.text[:100]}")
            return None
    except Exception as e:
        print(f"❌ Upload error: {e}")
        return None


# ── Scraping ─────────────────────────────────────────────
async def capture_cctv(url: str, location: str, retries: int = 3) -> dict:
    for attempt in range(1, retries + 1):
        url_hash  = hashlib.md5(url.encode()).hexdigest()[:8]
        timestamp = datetime.now(WIB).strftime("%Y%m%d_%H%M%S")
        filename  = f"{OUTPUT_DIR}/{location}_{url_hash}_{timestamp}.png"

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"]
            )
            context = await browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = await context.new_page()

            try:
                if attempt > 1:
                    print(f"  🔄 [{location}] Retry attempt {attempt}...", flush=True)
                else:
                    print(f"  🌐 [{location}] Navigating to page...", flush=True)
                                    
                await page.goto(url, timeout=45000, wait_until="domcontentloaded")
                print(f"  📄 [{location}] Page loaded, waiting 10s for stream...", flush=True)
                
                await page.wait_for_timeout(10000)   # base wait
                # extra guard: if loading screen text is still visible, wait a bit more
                loading_text = await page.locator("text=Your live stream will play automatically").count()
                if loading_text > 0:
                    print(f"  ⏳ [{location}] Stream still loading, waiting 5s more...", flush=True)
                    await page.wait_for_timeout(5000)

                has_error = False
                for pattern in ERROR_PATTERNS:
                    if await page.locator(f"text={pattern}").count() > 0:
                        has_error = True
                        break

                if has_error:
                    raise RuntimeError("video_not_found or codec error")

                print(f"  📸 [{location}] Taking screenshot...", flush=True)
                await page.screenshot(path=filename, full_page=False)
                print(f"  ✅ [{location}] Captured successfully", flush=True)

                return {
                    "status":    "ok",
                    "location":  location,
                    "url":       url,
                    "timestamp": datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"),
                    "filename":  filename,
                }

            except Exception as e:
                if attempt == retries:
                    print(f"  ❌ [{location}] Failed after {retries} attempt(s) — {str(e)[:80]}", flush=True)
                    return {
                        "status":    "error",
                        "reason":    str(e),
                        "location":  location,
                        "url":       url,
                        "timestamp": datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"),
                        "filename":  None,
                    }
                print(f"  ⚠️  [{location}] Attempt {attempt} failed, retrying in 2s...", flush=True)
                await asyncio.sleep(2)
            finally:
                await browser.close()


# ── YOLO Analysis ────────────────────────────────────────
def analyze_image(model, filename: str) -> dict:
    detections       = model(filename, conf=0.3, verbose=False)
    boxes            = detections[0].boxes
    image_h, image_w = detections[0].orig_shape
    total_image_area = image_w * image_h

    vehicle_counts = {label: 0 for label in VEHICLE_CLASSES.values()}
    covered_area   = 0

    for box in boxes:
        cls_id = int(box.cls)
        if cls_id not in VEHICLE_CLASSES:
            continue
        vehicle_counts[VEHICLE_CLASSES[cls_id]] += 1
        x1, y1, x2, y2 = box.xyxy[0].tolist()
        covered_area += (x2 - x1) * (y2 - y1)

    density        = round(covered_area / total_image_area, 4)
    total_vehicles = sum(v for k, v in vehicle_counts.items() if k in MOTORIZED_4W)

    return {
        "vehicle_count":     total_vehicles,
        "density":           density,
        "traffic_jam_label": density > DENSITY_THRESHOLD,
        "bicycle_count":     vehicle_counts["bicycle"],
        "car_count":         vehicle_counts["car"],
        "motorcycle_count":  vehicle_counts["motorcycle"],
        "bus_count":         vehicle_counts["bus"],
        "truck_count":       vehicle_counts["truck"],
    }


# ── Main ─────────────────────────────────────────────────
async def main():
    t_total = start_timer()

    # ── Test DB connection ──────────────────────────────
    print("🔌 Testing database connection...")
    try:
        conn = get_db_conn()
        conn.close()
        print("✅ Database connected")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return

    print("🤖 Loading YOLO model...")
    model = YOLO("yolo26n.pt")

    # ── 1. Scrape CCTV ─────────────────────────────────
    t0 = start_timer()
    print(f"📷 Scraping {len(CCTV_MAPPING)} cameras in parallel:", flush=True)
    for loc in CCTV_MAPPING.values():
        print(f"     → {loc}", flush=True)
    print(flush=True)

    results = await asyncio.gather(
        *[capture_cctv(url, location) for url, location in CCTV_MAPPING.items()]
    )
    log_timing("1. Scrape CCTV", t0)

    # ── 2-4. YOLO + Upload + Insert ────────────────────
    t_yolo   = 0
    t_upload = 0
    t_insert = 0

    for r in results:
        if r["status"] != "ok":
            print(f"⏭️  {r['location']} skipped — {r.get('reason')}")
            continue

        print(f"🔍 Analyzing {r['location']}...")

        # YOLO inference + annotation
        t0 = start_timer()
        metrics            = analyze_image(model, r["filename"])
        annotated_filename = r["filename"].replace(".png", "_annotated.jpg")
        model(r["filename"], conf=0.3, verbose=False)[0].save(filename=annotated_filename)
        t_yolo += time.time() - t0

        # Upload images via REST API
        t0                  = start_timer()
        image_url           = upload_image(r["filename"])
        annotated_image_url = upload_image(annotated_filename)
        t_upload           += time.time() - t0

        # Insert via psycopg2        
        t0     = start_timer()
        record = {
            "timestamp":           r["timestamp"],
            "location":            r["location"],
            "web_url":             r["url"],
            "image_url":           image_url,
            "annotated_image_url": annotated_image_url,
            "traffic_jam_label":   metrics["traffic_jam_label"],
            "density":             metrics["density"],
            "car_count":           metrics["car_count"],
            "motorcycle_count":    metrics["motorcycle_count"],
            "bus_count":           metrics["bus_count"],
            "truck_count":         metrics["truck_count"],
            "bicycle_count":       metrics["bicycle_count"],
            # ── calculate here directly, not from analyze_image ──
            "vehicle_count":       metrics["car_count"] + metrics["bus_count"] + metrics["truck_count"],
        }
        insert_full_row(record)
        t_insert += time.time() - t0

        jam_icon = "🔴" if metrics["traffic_jam_label"] else "🟢"
        print(f"   {jam_icon} density={metrics['density']*100:.1f}% | vehicles={metrics['vehicle_count']} | ✅ saved")

    TIMING["2. YOLO inference + annotation"]      = t_yolo
    TIMING["3. Upload images to Supabase"]        = t_upload
    TIMING["4. Insert rows to Supabase (upsert)"] = t_insert

    for step in ["2. YOLO inference + annotation",
                 "3. Upload images to Supabase",
                 "4. Insert rows to Supabase (upsert)"]:
        print(f"⏱️  {step}: {TIMING[step]:.2f}s")

    log_timing("5. TOTAL PIPELINE", t_total)

    # ── TIMING SUMMARY ─────────────────────────────────
    print("\n" + "═" * 50)
    print("  PIPELINE TIMING SUMMARY")
    print("═" * 50)

    total = TIMING.pop("5. TOTAL PIPELINE")
    for step, seconds in TIMING.items():
        pct = (seconds / total * 100) if total > 0 else 0
        print(f"  {step:<40s} {seconds:7.2f}s  ({pct:4.1f}%)")

    print("═" * 50)
    print(f"  TOTAL PIPELINE TIME: {total:.2f}s ({total/60:.2f} min)")
    print("═" * 50)
    print("\n✅ Pipeline complete")


if __name__ == "__main__":
    asyncio.run(main())