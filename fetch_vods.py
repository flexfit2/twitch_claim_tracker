import requests
import sqlite3
import os
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
load_dotenv()

# ===== CONFIG =====
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "chat.db")

CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
ACCESS_TOKEN = os.getenv("TWITCH_ACCESS_TOKEN")
USERNAME = os.getenv("TWITCH_USERNAME")
# ==================

HEADERS = {
    "Client-ID": CLIENT_ID,
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}


def get_user_id(username):
    r = requests.get(
        "https://api.twitch.tv/helix/users",
        headers=HEADERS,
        params={"login": username}
    )

    r.raise_for_status()
    data = r.json()

    if not data["data"]:
        raise RuntimeError("User hittades inte")

    return data["data"][0]["id"]


def get_all_vods(user_id):
    vods = []
    cursor = None

    while True:
        params = {
            "user_id": user_id,
            "first": 100,
            "type": "archive"
        }

        if cursor:
            params["after"] = cursor

        r = requests.get(
            "https://api.twitch.tv/helix/videos",
            headers=HEADERS,
            params=params
        )

        r.raise_for_status()
        data = r.json()

        for vid in data.get("data", []):
            vods.append({
                "id": vid["id"],
                "created_at": vid["created_at"],
                "duration": vid.get("duration"),
                "published_at": vid.get("published_at")
            })

        cursor = data.get("pagination", {}).get("cursor")
        if not cursor:
            break

    return vods


def get_processed_vods():
    if not os.path.exists(DB_PATH):
        return {}

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS processed_vods (
        vod_id TEXT PRIMARY KEY,
        processed_at DATETIME
    )
    """)

    cur.execute("SELECT vod_id, processed_at FROM processed_vods")
    rows = cur.fetchall()

    conn.close()

    return {row[0]: row[1] for row in rows}


def main():
    print("🔍 Hämtar user ID...")
    user_id = get_user_id(USERNAME)
    print("User ID:", user_id)

    print("📺 Hämtar alla VODs...")
    all_vods = get_all_vods(user_id)
    print("Totalt antal VODs:", len(all_vods))

    processed = get_processed_vods()
    print("Redan processade:", len(processed))

    new_vods = [v for v in all_vods if v not in processed]

    print("🆕 Nya VODs:", len(new_vods))

    for vod in new_vods:
        print(vod)


if __name__ == "__main__":
    main()
