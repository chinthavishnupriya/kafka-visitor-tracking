from kafka import KafkaConsumer
import json
import sqlite3

TOPIC = "website-visitors"
DB_NAME = "visitors.db"


# ============================================================
# DATABASE CONNECTION
# ============================================================

conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()


# ============================================================
# EVENT HISTORY TABLE
# Stores every page visit
# ============================================================

cursor.execute("""
CREATE TABLE IF NOT EXISTS visitor_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    visitor_id TEXT NOT NULL,
    page TEXT,
    ip_address TEXT,
    timestamp TEXT,
    event TEXT
)
""")


# ============================================================
# VISITOR SUMMARY TABLE
# Stores one record per unique visitor
# ============================================================

cursor.execute("""
CREATE TABLE IF NOT EXISTS visitors (
    visitor_id TEXT PRIMARY KEY,
    first_visit TEXT NOT NULL,
    last_visit TEXT NOT NULL,
    visit_count INTEGER DEFAULT 1
)
""")

conn.commit()


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=["localhost:9092"],
    group_id="visitor-summary-final",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)


print("=" * 70)
print("          KAFKA VISITOR DATABASE CONSUMER")
print("=" * 70)
print(f"Kafka Topic : {TOPIC}")
print(f"Database    : {DB_NAME}")
print("Waiting for visitor events...")
print()


# ============================================================
# PROCESS KAFKA EVENTS
# ============================================================

for message in consumer:

    event = message.value

    visitor_id = event["visitor_id"]
    page = event["page"]
    ip_address = event["ip_address"]
    timestamp = event["timestamp"]
    event_type = event["event"]


    # --------------------------------------------------------
    # 1. Store complete event history
    # --------------------------------------------------------

    cursor.execute("""
        INSERT INTO visitor_events
        (
            visitor_id,
            page,
            ip_address,
            timestamp,
            event
        )
        VALUES (?, ?, ?, ?, ?)
    """, (
        visitor_id,
        page,
        ip_address,
        timestamp,
        event_type
    ))


    # --------------------------------------------------------
    # 2. Check whether visitor already exists
    # --------------------------------------------------------

    cursor.execute("""
        SELECT visitor_id
        FROM visitors
        WHERE visitor_id = ?
    """, (visitor_id,))

    existing_visitor = cursor.fetchone()


    # --------------------------------------------------------
    # 3. New visitor
    # --------------------------------------------------------

    if existing_visitor is None:

        cursor.execute("""
            INSERT INTO visitors
            (
                visitor_id,
                first_visit,
                last_visit,
                visit_count
            )
            VALUES (?, ?, ?, ?)
        """, (
            visitor_id,
            timestamp,
            timestamp,
            1
        ))

        status = "NEW VISITOR"


    # --------------------------------------------------------
    # 4. Existing visitor
    # --------------------------------------------------------

    else:

        cursor.execute("""
            UPDATE visitors
            SET
                last_visit = ?,
                visit_count = visit_count + 1
            WHERE visitor_id = ?
        """, (
            timestamp,
            visitor_id
        ))

        status = "RETURNING VISITOR"


    conn.commit()


    # --------------------------------------------------------
    # Display result
    # --------------------------------------------------------

    print("-" * 70)
    print(status)
    print("-" * 70)

    print(f"Visitor ID : {visitor_id}")
    print(f"Page       : {page}")
    print(f"IP Address : {ip_address}")
    print(f"Timestamp  : {timestamp}")
    print(f"Event      : {event_type}")
    print(f"Partition  : {message.partition}")
    print(f"Offset     : {message.offset}")

    print()
