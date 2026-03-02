import sqlite3

DB_PATH = "chat.db"


def inspect_claim(claim_id):

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Hämta claim
    cur.execute("""
        SELECT vod_id,
               conversation_type,
               conversation_index,
               timestamp_seconds,
               claim_value
        FROM claims
        WHERE id = ?
    """, (claim_id,))

    row = cur.fetchone()

    if not row:
        print("Claim not found.")
        conn.close()
        return

    vod_id, conv_type, conv_index, claim_ts, claim_text = row

    print("\n" + "="*80)
    print(f"🔎 Inspecting Claim ID {claim_id}")
    print("="*80)
    print(f"VOD: {vod_id}")
    print(f"Conversation type: {conv_type}")
    print(f"Conversation index: {conv_index}")
    print(f"Timestamp: {claim_ts}")
    print(f"Claim text: {claim_text}")
    print("="*80)

    # Hämta hela konversationen
    cur.execute("""
        SELECT m.timestamp_seconds,
               m.username,
               m.clean_message
        FROM messages m
        JOIN conversation_messages cm
            ON m.id = cm.message_id
        JOIN conversations c
            ON cm.conversation_id = c.id
        WHERE c.vod_id = ?
          AND c.conversation_type = ?
          AND c.conversation_index = ?
        ORDER BY m.timestamp_seconds
    """, (vod_id, conv_type, conv_index))

    messages = cur.fetchall()

    print("\n🧵 Full Conversation Context:\n")

    for ts, user, msg in messages:
        marker = ">>>" if ts == claim_ts else "   "
        print(f"{marker} {ts:6} | {user:<15} | {msg}")

    conn.close()


if __name__ == "__main__":
    # Ändra till claim-id du vill inspektera
    inspect_claim(209)
