import sqlite3
import re
from collections import defaultdict, deque

DB_PATH = "chat.db"
TIME_WINDOW = 180  # max seconds back for a valid reply

mention_regex = re.compile(r"@([A-Za-z0-9_]{3,})")


def init_tables(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS conversations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vod_id TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS conversation_messages (
        conversation_id INTEGER,
        message_id INTEGER
    )
    """)

    conn.commit()


def build_reply_graph(messages):
    """
    Build adjacency list based on @mentions.
    Returns: dict {message_id: set(connected_message_ids)}
    """

    adjacency = defaultdict(set)

    # Track latest message per user (per VOD)
    latest_by_user = {}

    for msg_id, vod_id, username, ts, message in messages:

        mentions = mention_regex.findall(message)

        # Register edges for mentions
        for mentioned_user in mentions:
            key = (vod_id, mentioned_user)
            if key in latest_by_user:
                parent_id, parent_ts = latest_by_user[key]

                if ts - parent_ts <= TIME_WINDOW:
                    adjacency[msg_id].add(parent_id)
                    adjacency[parent_id].add(msg_id)

        # Update latest message from this user
        latest_by_user[(vod_id, username)] = (msg_id, ts)

    return adjacency


def extract_components(messages, adjacency):
    """
    Find connected components in reply graph.
    Returns list of sets of message_ids.
    """

    visited = set()
    components = []

    message_ids = [m[0] for m in messages]

    for mid in message_ids:
        if mid in visited:
            continue

        if mid not in adjacency:
            continue  # no edges

        queue = deque([mid])
        component = set()

        while queue:
            current = queue.popleft()
            if current in visited:
                continue

            visited.add(current)
            component.add(current)

            for neighbor in adjacency[current]:
                if neighbor not in visited:
                    queue.append(neighbor)

        if len(component) > 1:
            components.append(component)

    return components


def build_conversations():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    init_tables(conn)

    cur.execute("DELETE FROM conversations")
    cur.execute("DELETE FROM conversation_messages")
    conn.commit()

    cur.execute("""
        SELECT id, vod_id, username, timestamp_seconds, message
        FROM messages
        ORDER BY vod_id, timestamp_seconds ASC
    """)

    messages = cur.fetchall()

    print("🔗 Building reply graph...")
    adjacency = build_reply_graph(messages)

    print("🧠 Extracting connected components...")
    components = extract_components(messages, adjacency)

    print(f"💬 Found {len(components)} conversations.")

    for comp in components:
        # get vod_id from first message
        cur.execute("SELECT vod_id FROM messages WHERE id = ?", (next(iter(comp)),))
        vod_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO conversations (vod_id)
            VALUES (?)
        """, (vod_id,))
        convo_id = cur.lastrowid

        for mid in comp:
            cur.execute("""
                INSERT INTO conversation_messages (conversation_id, message_id)
                VALUES (?, ?)
            """, (convo_id, mid))

    conn.commit()
    conn.close()

    print("✅ Conversations built (graph-based).")


if __name__ == "__main__":
    build_conversations()   