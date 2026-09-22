# Kafka Real-Time Website Visitor Tracking & Analytics

A real-time website visitor tracking and analytics system built using **Python, Flask, Apache Kafka, SQLite, HTML, CSS, and JavaScript**.

The project demonstrates how website visitor activity can be captured as events, transmitted through Apache Kafka, processed by a Kafka consumer, stored in SQLite, and presented through a web-based analytics dashboard.

---

## Project Overview

The system tracks visitors who access a Flask-based website.

Whenever the website is opened or refreshed:

1. A unique visitor ID is generated for a new browser.
2. The visitor ID is stored in browser `localStorage`.
3. A `page_visit` event is created.
4. The event is sent to a Flask `/track` API.
5. The Flask producer publishes the event to Apache Kafka.
6. Kafka stores the event in the `website-visitors` topic.
7. A Python Kafka consumer receives the event.
8. The consumer stores the event in SQLite.
9. Visitor statistics are updated.
10. A separate Flask dashboard reads the SQLite database and displays analytics.

The project therefore demonstrates a complete **real-time event-processing pipeline**.

---

## System Architecture

```text
                    ┌─────────────────────┐
                    │   Web Browser       │
                    │ HTML/CSS/JavaScript │
                    └──────────┬──────────┘
                               │
                               │ Page Visit
                               ▼
                    ┌─────────────────────┐
                    │   Flask Producer    │
                    │      Port 5000      │
                    └──────────┬──────────┘
                               │
                               │ JSON Event
                               ▼
                    ┌─────────────────────┐
                    │    Apache Kafka     │
                    │                     │
                    │ website-visitors    │
                    │   3 Partitions      │
                    └──────────┬──────────┘
                               │
                               │ Consume Events
                               ▼
                    ┌─────────────────────┐
                    │  Python Consumer    │
                    │   Kafka Consumer    │
                    └──────────┬──────────┘
                               │
                               │ Store / Update
                               ▼
                    ┌─────────────────────┐
                    │   SQLite Database   │
                    │     visitors.db    │
                    └──────────┬──────────┘
                               │
                               │ Read Analytics
                               ▼
                    ┌─────────────────────┐
                    │ Flask Dashboard     │
                    │     Port 5001      │
                    └─────────────────────┘
