import subprocess
import sqlite3
import json
import os
import re
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

# ============================================================
# CONFIG
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLI_PATH = os.path.join(BASE_DIR, "bin/TwitchDownloaderCLI")
DB_PATH = os.getenv("DB_PATH", os.path.join(BASE_DIR, "chat.db"))
CHATS_DIR = os.path.join(BASE_DIR, "chats")
TARGET_USER = os.getenv("TARGET_USER")

ALIASES = os.getenv("TARGET_ALIASES", "").split(",")

CONTEXT_SECONDS = 60

DIRECT_SIGNALS = [
    s.strip().lower()
    for s in os.getenv("TOPIC_DIRECT_SIGNALS", "").split(",")
    if s.strip()
]

SOFT_SIGNALS = [
    s.strip().lower()
    for s in os.getenv("TOPIC_SOFT_SIGNALS", "").split(",")
    if s.strip()
]

# ============================================================
# DATABASE
# ============================================================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vod_id TEXT,
        username TEXT,
        timestamp_seconds INTEGER,
        message TEXT,
        clean_message TEXT,
        mention_type TEXT,
        created_at TEXT,
        UNIQUE(vod_id, username, timestamp_seconds, message)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS claims (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vod_id TEXT,
        conversation_type TEXT,
        conversation_index INTEGER,
        timestamp_seconds INTEGER,
        username TEXT,
        claim_type TEXT,
        claim_value TEXT,
        raw_message TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS processed_vods (
        vod_id TEXT PRIMARY KEY,
        processed_at DATETIME
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS conversations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vod_id TEXT,
        conversation_type TEXT,      -- "user" / "topic"
        conversation_index INTEGER,
        start_timestamp INTEGER,
        end_timestamp INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS conversation_messages (
        conversation_id INTEGER,
        message_id INTEGER
    )
    """)

    conn.commit()
    conn.close()

def mark_vod_processed(vod_id):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    INSERT OR REPLACE INTO processed_vods (vod_id, processed_at)
    VALUES (?, ?)
    """, (
        vod_id,
        datetime.now(timezone.utc).isoformat()
    ))

    conn.commit()
    conn.close()

# ============================================================
# DOWNLOAD
# ============================================================

import subprocess
import re
import sys


def download_chat(vod_id, output_path):
    cmd = [
        CLI_PATH,
        "chatdownload",
        "--id", vod_id,
        "-o", output_path
    ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    percent_pattern = re.compile(r"(\d+)%")

    for line in process.stdout:
        line = line.strip()

        match = percent_pattern.search(line)

        if match:
            percent = match.group(1)
            print(f"\r⬇ Downloading VOD {vod_id}... {percent}%", end="", flush=True)
        else:
            # skriv statusmeddelanden direkt
            print(f"\r{line}", end="", flush=True)

    process.wait()

    print(f"\r⬇ Downloading VOD {vod_id}... Done!        ")

    if process.returncode != 0:
        raise RuntimeError("TwitchDownloader failed")


# ============================================================
# INGESTION
# ============================================================

alias_pattern = re.compile(
    "(" + "|".join(re.escape(a) for a in ALIASES) + ")",
    re.IGNORECASE
)


def save_messages(vod_id, json_path):

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    comments = data["comments"]
    count = 0

    for comment in comments:

        created_at = comment.get("created_at")
        fragments = comment["message"]["fragments"]

        full_text = ""
        text_without_emotes = ""

        for frag in fragments:
            frag_text = frag["text"]
            full_text += frag_text

            if not frag.get("emoticon"):
                text_without_emotes += frag_text

        username = comment["commenter"]["display_name"]
        timestamp = comment["content_offset_seconds"]

        try:
            cur.execute("""
            INSERT INTO messages (
                vod_id,
                username,
                timestamp_seconds,
                message,
                clean_message,
                mention_type,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                vod_id,
                username,
                timestamp,
                full_text,
                text_without_emotes,
                None,
                created_at
            ))
            count += 1

        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()

    print(f"💾 Saved {count} messages")


# ============================================================
# LOW EFFORT FILTER
# ============================================================

def remove_mentions(text):
    return re.sub(r"^(@\w+\s*)+", "", text).strip()


def is_low_effort_message(clean_text):
    text = remove_mentions(clean_text).strip()
    words = text.split()

    if not text:
        return True

    if len(words) <= 2:
        return True

    avg_len = sum(len(w) for w in words) / len(words)
    if avg_len < 4:
        return True

    return False

def build_and_store_conversations(vod_id):

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Rensa ev gamla konversationer för VOD
    cur.execute("DELETE FROM conversation_messages WHERE conversation_id IN (SELECT id FROM conversations WHERE vod_id = ?)", (vod_id,))
    cur.execute("DELETE FROM conversations WHERE vod_id = ?", (vod_id,))
    conn.commit()

    # Hämta konversationer från dina befintliga extract-funktioner
    user_convs = extract_user_reply_conversations(vod_id)
    topic_convs = extract_reply_topic_conversations(vod_id)

    all_sets = [
        ("user", user_convs),
        ("topic", topic_convs)
    ]

    for conv_type, conv_list in all_sets:

        for index, message_ids in enumerate(conv_list, start=1):

            if not message_ids:
                continue

            # Hämta start / end timestamp
            placeholders = ",".join("?" for _ in message_ids)
            cur.execute(f"""
                SELECT MIN(timestamp_seconds), MAX(timestamp_seconds)
                FROM messages
                WHERE id IN ({placeholders})
            """, message_ids)

            start_ts, end_ts = cur.fetchone()

            cur.execute("""
                INSERT INTO conversations (
                    vod_id,
                    conversation_type,
                    conversation_index,
                    start_timestamp,
                    end_timestamp
                )
                VALUES (?, ?, ?, ?, ?)
            """, (vod_id, conv_type, index, start_ts, end_ts))

            conversation_id = cur.lastrowid

            # Koppla messages
            for mid in message_ids:
                cur.execute("""
                    INSERT INTO conversation_messages (
                        conversation_id,
                        message_id
                    )
                    VALUES (?, ?)
                """, (conversation_id, mid))

    conn.commit()
    conn.close()

    print(f"🧵 Stored {len(user_convs)} user + {len(topic_convs)} topic conversations")
    
# ============================================================
# MENTION HELPER
# ============================================================

def extract_leading_mentions(message, all_users):

    mentions = []

    # Explicit @mentions
    leading_match = re.match(r"^((@[A-Za-z0-9_]+\s*)+)", message)
    if leading_match:
        mentions.extend(
            re.findall(r"@([A-Za-z0-9_]+)", leading_match.group(1))
        )

    # Implicit mention (username as first token)
    tokens = message.split()
    if tokens:
        first_word = re.sub(r"[^\w_]", "", tokens[0])

        for user in all_users:
            if first_word.lower() == user.lower():
                if user not in mentions:
                    mentions.append(user)
                break

    return mentions


# ============================================================
# TOPIC CONVERSATIONS (ABOUT HIM)
# ============================================================

def extract_reply_topic_conversations(vod_id, topic_timeout=75):

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, timestamp_seconds, username, message, clean_message
        FROM messages
        WHERE vod_id = ?
        ORDER BY timestamp_seconds
    """, (vod_id,))

    rows = cur.fetchall()
    conn.close()

    if not rows:
        return []

    all_users = {row[2] for row in rows}

    def is_direct_signal(text):
        t = text.lower()
        return any(sig in t for sig in DIRECT_SIGNALS)

    def is_soft_signal(text):
        t = text.lower()
        return any(sig in t for sig in SOFT_SIGNALS)

    conversations = []
    current_conv = []
    participants = set()
    last_ts = None

    def close_current():
        nonlocal current_conv, participants, last_ts
        if current_conv:
            conversations.append(current_conv)
        current_conv = []
        participants = set()
        last_ts = None

    for msg_id, ts, username, message, clean_message in rows:

        if current_conv and last_ts is not None:
            if ts - last_ts > topic_timeout:
                close_current()

        direct = is_direct_signal(message)
        soft = is_soft_signal(message)
        mentions = extract_leading_mentions(message, all_users)

        if direct:

            if is_low_effort_message(clean_message):
                continue

            if not current_conv:
                current_conv = []
                participants = set()

            current_conv.append(msg_id)
            participants.add(username)
            last_ts = ts
            continue

        if current_conv:

            include = False

            if soft:
                include = True
            elif mentions:
                for m in mentions:
                    if m.lower() in {p.lower() for p in participants}:
                        include = True
                        break

            if not include:
                continue

            if is_low_effort_message(clean_message):
                continue

            current_conv.append(msg_id)
            participants.add(username)
            last_ts = ts

    if current_conv:
        conversations.append(current_conv)

    return conversations


# ============================================================
# USER CONVERSATIONS (WITH HIM)
# ============================================================

def extract_user_reply_conversations(vod_id, timeout=75):

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, timestamp_seconds, username, message, clean_message
        FROM messages
        WHERE vod_id = ?
        ORDER BY timestamp_seconds
    """, (vod_id,))

    rows = cur.fetchall()
    conn.close()

    if not rows:
        return []

    all_users = {row[2] for row in rows}

    conversations = []
    current_conv = []
    participants = set()
    last_ts = None

    def close_current():
        nonlocal current_conv, participants, last_ts
        if current_conv:
            conversations.append(current_conv)
        current_conv = []
        participants = set()
        last_ts = None

    for msg_id, ts, username, message, clean_message in rows:

        if current_conv and last_ts is not None:
            if ts - last_ts > timeout:
                close_current()

        mentions = extract_leading_mentions(message, all_users)

        is_from_target = username.lower() == TARGET_USER.lower()
        mentions_target = any(
            m.lower() == TARGET_USER.lower() for m in mentions
        )

        if is_from_target or mentions_target:

            if is_low_effort_message(clean_message):
                continue

            if not current_conv:
                current_conv = []
                participants = set()

            current_conv.append(msg_id)
            participants.add(username)
            last_ts = ts
            continue

        if current_conv:

            include = False

            for m in mentions:
                if m.lower() in {p.lower() for p in participants}:
                    include = True
                    break

            if not include:
                continue

            if is_low_effort_message(clean_message):
                continue

            current_conv.append(msg_id)
            participants.add(username)
            last_ts = ts

    if current_conv:
        conversations.append(current_conv)

    return conversations


# ============================================================
# CLAIM EXTRACTION (FROM USER CONVERSATIONS ONLY)
# ============================================================

SELF_REFERENCE_REGEX = re.compile(
    r"\b(i|i'm|i am|i was|i have|i've|i went|me|my|mine)\b",
    re.IGNORECASE
)

EXCLUDED_PREFIXES = [
    "i think",
    "i guess",
    "i feel",
    "i believe",
    "i mean",
    "i hope",
    "i wonder",
]

# ============================================================
# CLAIM CLEANUP HELPERS
# ============================================================

MIN_CLAIM_LENGTH = 15

EMOTE_PATTERN = re.compile(
    r"\b(?:[A-Z]{3,}|[a-z]+[A-Z][A-Za-z0-9]+)\b"
)

CONTINUATION_PATTERN = re.compile(
    r"^(?:\d+%?|and\b|but\b|so\b|because\b|also\b)",
    re.IGNORECASE
)


def remove_emotes(text):
    text = EMOTE_PATTERN.sub("", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def merge_claim_fragments(sentences):
    merged = []

    for s in sentences:
        s = s.strip()

        if (
            merged
            and len(s) < 25
            and CONTINUATION_PATTERN.match(s)
        ):
            merged[-1] = merged[-1] + " " + s
        else:
            merged.append(s)

    return merged


def is_valid_claim(sentence):
    if len(sentence) < MIN_CLAIM_LENGTH:
        return False
    return True

def extract_claims_from_user_conversations(vod_id):

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Rensa gamla claims för denna VOD (så vi slipper dubletter)
    cur.execute("""
        DELETE FROM claims
        WHERE vod_id = ?
        AND conversation_type = 'user'
    """, (vod_id,))
    conn.commit()

    # Hämta alla user-conversations
    cur.execute("""
        SELECT id, conversation_index
        FROM conversations
        WHERE vod_id = ?
        AND conversation_type = 'user'
        ORDER BY conversation_index
    """, (vod_id,))

    conversations = cur.fetchall()

    total_claims = 0

    for conversation_id, conv_index in conversations:

        cur.execute("""
            SELECT m.timestamp_seconds,
                   m.username,
                   m.clean_message,
                   m.created_at
            FROM messages m
            JOIN conversation_messages cm
                ON m.id = cm.message_id
            WHERE cm.conversation_id = ?
            ORDER BY m.timestamp_seconds
        """, (conversation_id,))

        messages = cur.fetchall()

        for ts, username, clean_text, created_at in messages:

            if username.lower() != TARGET_USER.lower():
                continue

            clean_text = re.sub(r"http\S+", "", clean_text)
            clean_text = re.sub(r"\s+", " ", clean_text).strip()
            clean_text = remove_emotes(clean_text)

            sentences = re.split(r"[.!?]+", clean_text)
            sentences = merge_claim_fragments(sentences)


            for sentence in sentences:

                sentence = sentence.strip()
                if not is_valid_claim(sentence):
                    continue

                lower_sentence = sentence.lower()

                # Måste innehålla själv-referens
                if not SELF_REFERENCE_REGEX.search(lower_sentence):
                    continue

                # Filtrera epistemiska prefix
                if any(lower_sentence.startswith(prefix) for prefix in EXCLUDED_PREFIXES):
                    continue

                cur.execute("""
                    INSERT INTO claims (
                        vod_id,
                        conversation_type,
                        conversation_index,
                        timestamp_seconds,
                        username,
                        claim_type,
                        claim_value,
                        raw_message,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    vod_id,
                    "user",
                    conv_index,
                    ts,
                    username,
                    "self_statement",
                    sentence,
                    clean_text,
                    created_at
                ))

                total_claims += 1

    conn.commit()
    conn.close()

    print(f"🧠 Extracted {total_claims} self-referential statements")

# ============================================================
# DEBUG
# ============================================================

def debug_all_conversations(vod_id):

    topic_convs = extract_reply_topic_conversations(vod_id)
    user_convs = extract_user_reply_conversations(vod_id)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print(f"\n🔎 Topic conversations: {len(topic_convs)}")
    print(f"🔎 User conversations: {len(user_convs)}")

    def print_convs(convs, label, conv_type=None):

        for i, conv in enumerate(convs, 1):

            print("\n" + "=" * 80)
            print(f"{label} #{i} ({len(conv)} messages)")
            print("=" * 80)

            placeholders = ",".join("?" for _ in conv)

            cur.execute(f"""
                SELECT timestamp_seconds, username, clean_message
                FROM messages
                WHERE id IN ({placeholders})
                ORDER BY timestamp_seconds
            """, conv)

            for ts, user, msg in cur.fetchall():
                print(f"{ts:6} | {user:<15} | {msg}")

            # ---- PRINT CLAIMS (only for user conversations) ----
            if conv_type == "user":

                cur.execute("""
                    SELECT timestamp_seconds, claim_type, claim_value
                    FROM claims
                    WHERE vod_id = ?
                    AND conversation_type = ?
                    AND conversation_index = ?
                    ORDER BY timestamp_seconds
                """, (vod_id, "user", i - 1))

                claims = cur.fetchall()

                if claims:
                    print("\n   🧠 CLAIMS FOUND:")
                    for ts, ctype, value in claims:
                        print(f"      [{ts}] ({ctype}) → {value}")
                else:
                    print("\n   🧠 No claims found.")

    # Print topic convs (no claims here)
    print_convs(topic_convs, "🧵 Topic Conversation")

    # Print user convs + claims
    print_convs(user_convs, "💬 User Conversation", conv_type="user")

    conn.close()

def print_user_conversation_report(vod_id):

    user_convs = extract_user_reply_conversations(vod_id)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

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

        # Claims
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

    vod_id = "2686219066"
    json_path = os.path.join(CHATS_DIR, f"{vod_id}.json")

    init_db()
    save_messages(vod_id, json_path)
    build_and_store_conversations(vod_id)
    extract_claims_from_user_conversations(vod_id)
    debug_all_conversations(vod_id)