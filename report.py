import sqlite3
from collector import (
    extract_user_reply_conversations,
    DB_PATH,
)


def print_user_conversation_report(vod_id):

    user_convs = extract_user_reply_conversations(vod_id)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print(f"\n🔎 {len(user_convs)} user conversations found")

    for i, conv in enumerate(user_convs, 1):

        print("\n" + "=" * 80)
        print(f"💬 User Conversation #{i} ({len(conv)} messages)")
        print("=" * 80)

        placeholders = ",".join("?" for _ in conv)

        cur.execute(f"""
            SELECT timestamp_seconds, username, message
            FROM messages
            WHERE id IN ({placeholders})
            ORDER BY timestamp_seconds
        """, conv)

        for ts, user, msg in cur.fetchall():
            print(f"{ts:6} | {user:<15} | {msg}")

        # Print claims
        cur.execute("""
            SELECT timestamp_seconds, claim_value
            FROM claims
            WHERE vod_id = ?
            AND conversation_type = 'user'
            AND conversation_index = ?
            ORDER BY timestamp_seconds
        """, (vod_id, i - 1))

        claims = cur.fetchall()

        if claims:
            print("\n   🧠 CLAIMS FOUND:")
            for ts, value in claims:
                print(f"      [{ts}] → {value}")

    conn.close()


if __name__ == "__main__":

    vod_id = input("Enter VOD ID: ").strip()
    print_user_conversation_report(vod_id)
