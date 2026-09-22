# Kafka Real-Time Website Visitor Tracking & Analytics

A real-time website visitor tracking and analytics system built using
Flask, Apache Kafka, Python, SQLite, and HTML/CSS/JavaScript.

## Project Overview

This project records website visitor activity and processes the events
through Apache Kafka.

Each visitor receives a unique visitor ID. When the visitor opens the
website, a `page_visit` event is generated and sent to Kafka.

The Kafka consumer receives these events and stores them in SQLite.
A separate Flask dashboard reads the database and displays real-time
visitor analytics.

## Architecture

Browser
   |
   v
Flask Producer
   |
   v
Apache Kafka
   |
   v
website-visitors Topic
   |
   v
Python Kafka Consumer
   |
   v
SQLite Database
   |
   v
Flask Analytics Dashboard


## Technologies Used

- Python 3
- Flask
- Apache Kafka
- kafka-python-ng
- SQLite
- HTML
- CSS
- JavaScript
- Linux / Ubuntu


## Main Components

### 1. Producer

Location:

`producer/app.py`

Responsibilities:

- Runs the visitor website
- Generates visitor IDs
- Stores visitor ID in browser localStorage
- Sends visitor events to Kafka
- Provides the `/track` API endpoint


### 2. Kafka

Kafka topic:

`website-visitors`

Kafka receives visitor events from the Flask producer.

Example event:

```json
{
    "visitor_id": "visitor-example-123",
    "page": "/",
    "ip_address": "172.23.16.1",
    "timestamp": "2026-09-22T08:52:43+00:00",
    "event": "page_visit"
}
