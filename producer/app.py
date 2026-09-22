from flask import Flask, request, jsonify, render_template_string
from kafka import KafkaProducer
import json
from datetime import datetime, timezone

app = Flask(__name__)

# ============================================================
# KAFKA CONFIGURATION
# ============================================================

KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "website-visitors"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ============================================================
# WEBSITE PAGE
# ============================================================

HTML_PAGE = """
<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">

    <title>Kafka Visitor Tracking</title>

    <style>
        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            min-height: 100vh;
            font-family: Arial, sans-serif;
            background: #f3f6f9;

            display: flex;
            justify-content: center;
            align-items: center;
        }

        .container {
            width: 90%;
            max-width: 900px;

            background: white;

            padding: 45px;

            border-radius: 18px;

            box-shadow:
                0 10px 30px rgba(0, 0, 0, 0.10);
        }

        h1 {
            margin-top: 0;
            font-size: 42px;
            color: #111827;
        }

        p {
            font-size: 20px;
            color: #374151;
        }

        .visitor-box {
            margin-top: 30px;

            padding: 20px;

            background: #eef2f7;

            border-radius: 10px;

            font-family: monospace;

            font-size: 16px;

            word-break: break-all;
        }

        .status {
            margin-top: 20px;

            font-size: 15px;

            color: #16a34a;
        }
    </style>
</head>

<body>

<div class="container">

    <h1>Welcome to My Website</h1>

    <p>
        This website records visitors using Kafka.
    </p>

    <div class="visitor-box">
        Visitor ID:
        <span id="visitor-id">Creating visitor ID...</span>
    </div>

    <div class="status" id="status">
        Connecting to Kafka tracking system...
    </div>

</div>


<script>

    // ========================================================
    // GENERATE A VISITOR ID
    // ========================================================
    //
    // We intentionally DO NOT use crypto.randomUUID()
    // because some browser/environment combinations can
    // produce:
    //
    //     crypto.randomUUID is not a function
    //
    // ========================================================

    function generateVisitorId() {

        var timestamp = Date.now().toString(36);

        var randomPart =
            Math.random().toString(36).substring(2, 12);

        return "visitor-" + timestamp + "-" + randomPart;
    }


    // ========================================================
    // GET OR CREATE VISITOR ID
    // ========================================================

    function getVisitorId() {

        var visitorId = null;

        try {

            visitorId = localStorage.getItem("kafka_visitor_id");

            if (!visitorId) {

                visitorId = generateVisitorId();

                localStorage.setItem(
                    "kafka_visitor_id",
                    visitorId
                );
            }

        } catch (error) {

            // If localStorage is unavailable,
            // still create an ID.

            visitorId = generateVisitorId();
        }

        return visitorId;
    }


    // ========================================================
    // TRACK VISITOR
    // ========================================================

    function trackVisitor() {

        var visitorId = getVisitorId();

        // Display visitor ID immediately

        document.getElementById("visitor-id").textContent =
            visitorId;


        // Send visitor event to Flask

        fetch("/track", {

            method: "POST",

            headers: {
                "Content-Type": "application/json"
            },

            body: JSON.stringify({

                visitor_id: visitorId,

                page: window.location.pathname

            })

        })

        .then(function(response) {

            return response.json();

        })

        .then(function(data) {

            console.log(
                "Visitor event sent to Kafka:",
                data
            );

            document.getElementById("status").textContent =
                "Visitor event recorded successfully.";
        })

        .catch(function(error) {

            console.error(
                "Visitor tracking error:",
                error
            );

            document.getElementById("status").textContent =
                "Visitor tracking failed.";
        });
    }


    // ========================================================
    // START TRACKING AFTER PAGE LOAD
    // ========================================================

    window.addEventListener("load", function() {

        trackVisitor();

    });

</script>

</body>
</html>
"""


# ============================================================
# HOME PAGE
# ============================================================

@app.route("/", methods=["GET"])
def home():

    return render_template_string(HTML_PAGE)


# ============================================================
# TRACK VISITOR
# ============================================================

@app.route("/track", methods=["POST"])
def track():

    try:

        data = request.get_json()

        if not data:

            return jsonify({
                "status": "error",
                "message": "No JSON data received"
            }), 400


        visitor_id = data.get("visitor_id")

        page = data.get("page", "/")


        if not visitor_id:

            return jsonify({
                "status": "error",
                "message": "visitor_id is required"
            }), 400


        # Get visitor IP address

        ip_address = request.remote_addr


        # Create UTC timestamp

        timestamp = datetime.now(
            timezone.utc
        ).isoformat()


        # Create Kafka event

        event = {

            "visitor_id": visitor_id,

            "page": page,

            "ip_address": ip_address,

            "timestamp": timestamp,

            "event": "page_visit"

        }


        # Send event to Kafka

        future = producer.send(
            KAFKA_TOPIC,
            value=event
        )


        # Wait until Kafka confirms the message

        metadata = future.get(timeout=10)


        # Make sure message is written

        producer.flush()


        print(
            "Visitor event sent to Kafka:",
            event
        )


        return jsonify({

            "status": "success",

            "visitor_id": visitor_id,

            "topic": metadata.topic,

            "partition": metadata.partition,

            "offset": metadata.offset

        }), 200


    except Exception as e:

        print(
            "ERROR while sending visitor event:",
            str(e)
        )


        return jsonify({

            "status": "error",

            "message": str(e)

        }), 500


# ============================================================
# RUN FLASK SERVER
# ============================================================

if __name__ == "__main__":

    print("=" * 70)

    print("           KAFKA VISITOR TRACKING PRODUCER")

    print("=" * 70)

    print("Kafka Broker :", KAFKA_BROKER)

    print("Kafka Topic  :", KAFKA_TOPIC)

    print("Flask Port   : 5000")

    print("=" * 70)


    app.run(

        host="0.0.0.0",

        port=5000,

        debug=True

    )
