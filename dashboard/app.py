from flask import Flask, render_template
import sqlite3

app = Flask(__name__)

DATABASE = "../consumer/visitors.db"


def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/")
def dashboard():

    conn = get_db()
    cursor = conn.cursor()

    # ---------------------------------------------------------
    # TOTAL EVENTS
    # ---------------------------------------------------------

    cursor.execute("""
        SELECT COUNT(*) AS total
        FROM visitor_events
    """)

    total_events = cursor.fetchone()["total"]


    # ---------------------------------------------------------
    # UNIQUE VISITORS
    # ---------------------------------------------------------

    cursor.execute("""
        SELECT COUNT(*) AS total
        FROM visitors
    """)

    unique_visitors = cursor.fetchone()["total"]


    # ---------------------------------------------------------
    # TOTAL VISITS
    # ---------------------------------------------------------

    cursor.execute("""
        SELECT COALESCE(SUM(visit_count), 0) AS total
        FROM visitors
    """)

    total_visits = cursor.fetchone()["total"]


    # ---------------------------------------------------------
    # TODAY'S UNIQUE VISITORS
    # ---------------------------------------------------------

    cursor.execute("""
        SELECT COUNT(DISTINCT visitor_id) AS total
        FROM visitor_events
        WHERE date(timestamp) = date('now')
    """)

    today_visitors = cursor.fetchone()["total"]


    # ---------------------------------------------------------
    # VISITOR SUMMARY
    # ---------------------------------------------------------

    cursor.execute("""
        SELECT
            visitor_id,
            first_visit,
            last_visit,
            visit_count
        FROM visitors
        ORDER BY last_visit DESC
    """)

    visitors = cursor.fetchall()


    # ---------------------------------------------------------
    # PAGE STATISTICS
    # ---------------------------------------------------------

    cursor.execute("""
        SELECT
            page,
            COUNT(*) AS views,
            COUNT(DISTINCT visitor_id) AS unique_visitors
        FROM visitor_events
        GROUP BY page
        ORDER BY views DESC
    """)

    page_stats = cursor.fetchall()


    # ---------------------------------------------------------
    # RECENT EVENTS
    # ---------------------------------------------------------

    cursor.execute("""
        SELECT
            id,
            visitor_id,
            page,
            ip_address,
            timestamp,
            event
        FROM visitor_events
        ORDER BY timestamp DESC
        LIMIT 15
    """)

    recent_events = cursor.fetchall()


    conn.close()


    return render_template(
        "dashboard.html",
        total_events=total_events,
        unique_visitors=unique_visitors,
        total_visits=total_visits,
        today_visitors=today_visitors,
        visitors=visitors,
        page_stats=page_stats,
        recent_events=recent_events
    )


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5001,
        debug=True
    )
