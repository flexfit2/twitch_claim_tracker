import sqlite3
import re
from datetime import datetime
from collections import defaultdict

DB_PATH = "chat.db"

YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")
RANGE_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\s*-\s*(19\d{2}|20\d{2})\b")
AGE_PATTERN = re.compile(r"\bwhen i was (\d{1,2})\b")
TURN_PATTERN = re.compile(r"\bi turn (\d{1,2})\b")
IM_PATTERN = re.compile(
    r"\b(i am|i'm|im) (\d{1,2})(?!\s*(years|hours|minutes|beers))\b"
)
YEARS_AGO_PATTERN = re.compile(r"\b(\d{1,2}) years ago\b")


# -------------------------------
# Helpers
# -------------------------------

def extract_temporal_data(text):
    return {
        "years": [int(y) for y in YEAR_PATTERN.findall(text)],
        "ranges": [(int(a), int(b)) for a, b in RANGE_PATTERN.findall(text)],
        "ages": [int(a) for a in AGE_PATTERN.findall(text)],
        "turn": [int(a) for a in TURN_PATTERN.findall(text)],
        "im_age": [int(m[1]) for m in IM_PATTERN.findall(text)],
        "years_ago": [int(x) for x in YEARS_AGO_PATTERN.findall(text)],
    }


def is_personal_year(text, year):

    sentences = re.split(r"[.!?]", text)

    for s in sentences:
        if str(year) in s:
            s = s.lower()

            if any(
                phrase in s
                for phrase in [
                    "i was",
                    "when i",
                    "i remember",
                    "i served",
                    "i went",
                    "i lived",
                    "i had",
                ]
            ):
                return True

    return False


def estimate_birth_year(claims):
    candidates = []

    for claim in claims:
        text = claim["claim_value"].lower()
        created_at = claim["created_at"]
        if not created_at:
            continue

        stream_year = datetime.fromisoformat(
            created_at.replace("Z", "")
        ).year

        data = extract_temporal_data(text)

        for age in data["ages"]:
            if 5 <= age <= 80:
                candidates.append(stream_year - age)

        for age in data["turn"]:
            if 15 <= age <= 80:
                candidates.append(stream_year - age)

        for age in data["im_age"]:
            if 15 <= age <= 80:
                candidates.append(stream_year - age)

    if not candidates:
        return None

    candidates.sort()
    return candidates[len(candidates)//2]


# -------------------------------
# Main
# -------------------------------

def main():

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT vod_id, timestamp_seconds, claim_value, created_at
        FROM claims
        WHERE claim_type = 'self_statement'
    """)

    rows = cur.fetchall()
    conn.close()

    claims = [
        {
            "vod_id": r[0],
            "timestamp_seconds": r[1],
            "claim_value": r[2],
            "created_at": r[3]
        }
        for r in rows
    ]

    birth_year = estimate_birth_year(claims)

    print("\n==============================")
    print("🧬 ESTIMATED DEMOGRAPHIC BASELINE")
    print("==============================")

    if birth_year:
        print(f"Estimated birth year: ~{birth_year}")
    else:
        print("Could not estimate birth year")

    timeline = defaultdict(list)
    ranges = []

    for claim in claims:

        text = claim["claim_value"]
        lower = text.lower()
        created_at = claim["created_at"]
        if not created_at:
            continue

        stream_year = datetime.fromisoformat(
            created_at.replace("Z", "")
        ).year

        data = extract_temporal_data(lower)

        # Absolute years
        for year in data["years"]:
            if birth_year and year < birth_year:
                continue

            if not is_personal_year(lower, year):
                continue

            timeline[year].append(claim)

        # Ranges
        for start, end in data["ranges"]:
            if birth_year and start < birth_year:
                continue
            ranges.append((start, end, claim))

        # Years ago
        for yrs in data["years_ago"]:
            year = stream_year - yrs
            if birth_year and year < birth_year:
                continue
            timeline[year].append({
                **claim,
                "claim_value": f"({yrs} years ago) {text}"
            })

    # -------------------------------
    # Timeline Output
    # -------------------------------

    print("\n==============================")
    print("🕰 LIFE TIMELINE")
    print("==============================")

    for year in sorted(timeline.keys()):
        print(f"\n🔹 {year} ({len(timeline[year])} events)")
        for c in timeline[year][:5]:
            print(f"   [{c['vod_id']} @ {c['timestamp_seconds']}] {c['claim_value']}")

    # -------------------------------
    # Range Analysis
    # -------------------------------

    print("\n==============================")
    print("📏 SERVICE / PERIOD RANGES")
    print("==============================")

    for start, end, claim in ranges:
        print(f"   {start}-{end} → {claim['claim_value']}")

    # Conflict detection
    print("\n==============================")
    print("⚠ RANGE CONFLICT CHECK")
    print("==============================")

    for i in range(len(ranges)):
        for j in range(i+1, len(ranges)):
            a1, b1, c1 = ranges[i]
            a2, b2, c2 = ranges[j]

            # overlap or suspicious adjacency
            if not (b1 < a2 or b2 < a1):
                print(f"\n⚠ Overlap detected:")
                print(f"   {a1}-{b1}")
                print(f"   {a2}-{b2}")

    # -------------------------------
    # Biographical summary
    # -------------------------------

    print("\n==============================")
    print("📖 BIOGRAPHICAL SUMMARY")
    print("==============================")

    if birth_year:
        print(f"Born approx: {birth_year}")

    if ranges:
        service_ranges = [
            r for r in ranges
            if any(
                keyword in r[2]["claim_value"].lower()
                for keyword in ["serve", "military", "combat", "jäger", "tour"]
            )
        ]

        if service_ranges:
            earliest = min(r[0] for r in service_ranges)
            latest = max(r[1] for r in service_ranges)

        latest = max(r[1] for r in ranges)
        print(f"Active claimed service span: {earliest}-{latest}")

    if timeline:
        first_event = min(timeline.keys())
        print(f"Earliest personal event: {first_event}")

        recent_event = max(timeline.keys())
        print(f"Most recent referenced year: {recent_event}")


if __name__ == "__main__":
    main()
