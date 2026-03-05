import os
import argparse
from datetime import datetime, timezone, timedelta
import glob

from fetch_vods import get_user_id, get_all_vods, get_processed_vods
from collector import (
    init_db,
    download_chat,
    save_messages,
    build_and_store_conversations,
    extract_claims_from_user_conversations,
    mark_vod_processed,
    CHATS_DIR,
)

from dotenv import load_dotenv
load_dotenv()

USERNAME = os.getenv("TWITCH_USERNAME")

# Live VOD settings
RECHECK_WINDOW = timedelta(hours=48)
RECHECK_COOLDOWN = timedelta(hours=6)


# ---------------------------------------------------
# Helpers
# ---------------------------------------------------

def get_latest_json_for_vod(vod_id):
    matches = []

    for root, _, files in os.walk(CHATS_DIR):
        for f in files:
            if f.startswith(vod_id) and f.endswith(".json"):
                matches.append(os.path.join(root, f))

    if not matches:
        return None

    return max(matches, key=os.path.getmtime)


def create_archive_path(vod_id):
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
    year_folder = now.strftime("%Y")

    archive_dir = os.path.join(CHATS_DIR, year_folder)
    os.makedirs(archive_dir, exist_ok=True)

    return os.path.join(
        archive_dir,
        f"{vod_id}_{timestamp}.json"
    )


# ---------------------------------------------------
# Main
# ---------------------------------------------------

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--force-all", action="store_true")
    parser.add_argument("--force", type=str, help="Force update specific VOD ID")
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip fetching/downloading VOD chat and reuse existing JSON files"
    )
    args = parser.parse_args()

    print("🔧 Init DB...")
    init_db()

    os.makedirs(CHATS_DIR, exist_ok=True)

    print("🔍 Fetching Twitch VOD list...")

    if not args.skip_fetch:
        user_id = get_user_id(USERNAME)
        twitch_vods = get_all_vods(user_id)
    else:
        print("⏩ Skip-fetch mode enabled. Reusing local JSON files.")
        json_files = glob.glob(os.path.join(CHATS_DIR, "*.json"))
        twitch_vods = []

        for f in json_files:
            vod_id = os.path.splitext(os.path.basename(f))[0]
            twitch_vods.append({
                "id": vod_id,
                # fake timestamp – används bara för jämförelser
                "created_at": None
            })

    print(f"🆕 {len(twitch_vods)} VODs hittades.")

    processed = get_processed_vods()
    now = datetime.now(timezone.utc)

    for vod in twitch_vods:

        vod_id = vod["id"]
        print(f"\n=== Processing VOD {vod_id} ===")

        if args.force and vod_id != args.force:
            continue

        if not args.skip_fetch:
            created_at = datetime.fromisoformat(
                vod["created_at"].replace("Z", "+00:00")
            )
            age = now - created_at
        else:
            created_at = None
            age = None

        latest_json = get_latest_json_for_vod(vod_id)
        last_checked = processed.get(vod_id)

        if last_checked:
            last_checked = datetime.fromisoformat(last_checked)
            if last_checked.tzinfo is None:
                last_checked = last_checked.replace(tzinfo=timezone.utc)

        should_download = False

        # --------------------------------------------
        # FORCE MODES
        # --------------------------------------------

        if args.force_all:
            print("🔥 Force mode: updating")
            should_download = True

        elif args.force:
            print("🔥 Force mode: updating")
            should_download = True
            
        if args.skip_fetch:
            should_download = False

        # --------------------------------------------
        # NORMAL MODE
        # --------------------------------------------

        else:

            # No JSON at all → must download
            if not latest_json:
                print("⬇ No archive found, downloading chat...")
                should_download = True

            # Recent VOD → periodic refresh
            elif not args.skip_fetch and age < RECHECK_WINDOW:

                if not last_checked:
                    should_download = True
                elif now - last_checked > RECHECK_COOLDOWN:
                    print("🔄 Live VOD recheck triggered")
                    should_download = True

        # --------------------------------------------
        # DOWNLOAD (if needed)
        # --------------------------------------------

        if should_download:
            archive_path = create_archive_path(vod_id)
            print("⬇ Downloading chat...")
            download_chat(vod_id, archive_path)
            latest_json = get_latest_json_for_vod(vod_id)

        if not latest_json:
            print("⚠ No JSON found, skipping.")
            continue

        # --------------------------------------------
        # STATE-DRIVEN PROCESSING
        # --------------------------------------------

        json_mtime = datetime.fromtimestamp(
            os.path.getmtime(latest_json),
            tz=timezone.utc
        )

        needs_processing = (
            not last_checked or json_mtime > last_checked
        )

        if not needs_processing:
            print("⏭ No changes detected.")
            continue

        print("💾 Saving messages...")
        save_messages(vod_id, latest_json)

        print("🧵 Building conversations...")
        build_and_store_conversations(vod_id)

        print("🧠 Extracting claims...")
        extract_claims_from_user_conversations(vod_id)

        mark_vod_processed(vod_id)

        print("✅ Done.")

        if args.force:
            break

    print("\n🎉 Pipeline complete.")


if __name__ == "__main__":
    main()