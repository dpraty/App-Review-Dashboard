import os
from datetime import datetime
from google_play_scraper import Sort, reviews
from supabase import create_client, Client

# 1. Pipeline Settings
APP_ID = "com.spotify.music"
BATCH_SIZE = 50

# 2. Dynamic Environment Variables
# This looks for variables set by your machine or GitHub actions.
# If it can't find them, it defaults to an empty string.
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

print("Initializing connection to Supabase via Service Role...")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

print("Initializing connection to Supabase...")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

print(f"Fetching latest {BATCH_SIZE} reviews for {APP_ID}...")
scraped_reviews, _ = reviews(
    APP_ID, lang="en", country="us", sort=Sort.NEWEST, count=BATCH_SIZE
)

print(f"Found {len(scraped_reviews)} raw records. Formatting data fields...")

# 3. Transform the data to match your database schema
db_records = []
for r in scraped_reviews:
    # Convert string datetime data into standard ISO formats
    review_date = r.get("at")
    iso_date = (
        review_date.isoformat()
        if isinstance(review_date, datetime)
        else str(review_date)
    )

    record = {
        "reviewId": r.get("reviewId"),  # Text primary key
        "app_id": APP_ID,  # Text identification
        "review_text": r.get("content"),  # Text content
        "rating": int(r.get("score")),  # Integer rating
        "app_version": r.get("reviewCreatedVersion", "N/A"),  # Text version indicator
        "likes": int(r.get("thumbsUpCount", 0)),  # Integer count
        "review_date": iso_date,  # Timestamp with timezone
    }
    db_records.append(record)

# 4. Push the formatted batch to Supabase
if db_records:
    print(f"Streaming {len(db_records)} records to Supabase tables...")
    try:
        # .upsert() inserts new rows, or skips if the 'id' already exists
        response = supabase.table("app_reviews").upsert(db_records).execute()
        print("Pipeline Execution Complete! Data successfully streamed.")
    except Exception as e:
        print(f"\n[CRITICAL ERROR] Failed to push to Supabase: {e}")
        print(
            "Verify your table columns exactly match the dictionary keys used in this script."
        )
else:
    print("Zero records found to process.")
