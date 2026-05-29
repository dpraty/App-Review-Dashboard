import os
import time
import logging
from datetime import datetime, timedelta
from google_play_scraper import Sort, reviews, app
from supabase import create_client, Client

# ==========================================
# 1. PIPELINE CONFIGURATION & SETUP
# ==========================================
# Replaced standard print() with standard Python logging for better GitHub Actions tracking
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

TARGET_APPS = {
    "Spotify": "com.spotify.music",
    "YouTube Music": "com.google.android.apps.youtube.music",
    "Apple Music": "com.apple.android.music",
    "Amazon Music": "com.amazon.mp3",
}

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

logging.info("Initializing connection to Supabase via Service Role...")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Define the absolute 30-day cutoff boundary
# Note: google-play-scraper returns naive datetimes. GitHub Actions runs in UTC by default.
CUTOFF_DATE = datetime.now() - timedelta(days=30)
CUTOFF_ISO = CUTOFF_DATE.isoformat()

# ==========================================
# 2. INGESTION & DATA SYNC LOOP
# ==========================================
for app_name, app_id in TARGET_APPS.items():
    logging.info(f"========== Processing {app_name} ({app_id}) ==========")

    # ---- PHASE 1: SYNC THE GLOBAL UNIVERSE MASTER METADATA ----
    try:
        logging.info(f"Fetching global storefront metadata for {app_name}...")
        meta = app(app_id, lang="en", country="us")

        universe_data = {
            "app_id": app_id,
            "app_name": app_name,
            "overall_rating": float(meta.get("score", 0)),
            "total_reviews": int(meta.get("reviews", 0)),
            "updated_at": datetime.now().isoformat(),
        }

        supabase.table("app_universe").upsert(universe_data).execute()
        logging.info(
            f"Success! Updated universe metrics (Global Avg: {universe_data['overall_rating']})"
        )

    except Exception as e:
        logging.error(f"[METADATA ERROR] Failed to sync universe for {app_name}: {e}")

    # ---- PHASE 2: INCREMENTAL PAGINATION FOR ROLLING REVIEWS ----
    logging.info(f"Initiating review scrape for {app_name}...")
    continuation_token = None
    total_upserted = 0

    # 1. FETCH THE HIGH-WATER MARK FROM SUPABASE
    # We ask the DB: What is the most recent review_date for this specific app?
    latest_record = (
        supabase.table("app_reviews")
        .select("review_date")
        .eq("app_id", app_id)
        .order("review_date", desc=True)
        .limit(1)
        .execute()
    )

    # 2. SET THE DYNAMIC CUTOFF
    if latest_record.data:
        # We have existing data! Set the cutoff to our newest existing review.
        db_date_str = latest_record.data[0]["review_date"].replace("Z", "+00:00")
        dynamic_cutoff = datetime.fromisoformat(db_date_str).replace(tzinfo=None)
        logging.info(
            f"Existing data found. Incremental sync cutoff set to: {dynamic_cutoff}"
        )
    else:
        # First run, or paused project with an empty DB. Fallback to 30 days.
        dynamic_cutoff = CUTOFF_DATE
        logging.info(
            f"No existing data. Full 30-day sync cutoff set to: {dynamic_cutoff}"
        )

    # 3. THE SMART LOOP
    while True:
        try:
            scraped_reviews, continuation_token = reviews(
                app_id,
                continuation_token=continuation_token,
                lang="en",
                country="us",
                sort=Sort.NEWEST,
                count=200,
            )

            db_records = []
            reached_cutoff = False

            for r in scraped_reviews:
                review_date = r.get("at")

                # THE BREAK CONDITION: Stop if we hit a review older than our dynamic cutoff
                if review_date <= dynamic_cutoff:
                    reached_cutoff = True
                    break

                db_records.append(
                    {
                        "reviewId": r.get("reviewId"),
                        "app_id": app_id,
                        "review_text": r.get("content"),
                        "rating": int(r.get("score")),
                        "app_version": r.get("reviewCreatedVersion", "N/A"),
                        "likes": int(r.get("thumbsUpCount", 0)),
                        "review_date": review_date.isoformat(),
                    }
                )

            if db_records:
                supabase.table("app_reviews").upsert(db_records).execute()
                total_upserted += len(db_records)
                logging.info(f"  ... Upserted batch of {len(db_records)} new records.")

            if reached_cutoff:
                logging.info(
                    f"Reached existing data boundary for {app_name}. Sync complete."
                )
                break
            if not continuation_token:
                logging.info(
                    f"Reached the absolute end of Play Store reviews for {app_name}."
                )
                break

            time.sleep(1.5)

        except Exception as e:
            logging.error(
                f"[REVIEW ERROR] Failure during pagination for {app_name}: {e}"
            )
            break

    logging.info(f"Finished {app_name}. Total new reviews processed: {total_upserted}")

# ==========================================
# 3. ROLLING WINDOW DATA RETENTION POLICY (TTL)
# ==========================================
logging.info("\n========== Executing Data Retention Policy ==========")
logging.info(f"Purging all database reviews strictly older than: {CUTOFF_ISO}")

try:
    purge_action = (
        supabase.table("app_reviews").delete().lt("review_date", CUTOFF_ISO).execute()
    )
    logging.info(
        "Success! Historical stragglers cleanly purged from app_reviews table."
    )
except Exception as e:
    logging.error(f"[RETENTION ERROR] Data purge failed: {e}")

logging.info("Pipeline Task Execution Complete!")
