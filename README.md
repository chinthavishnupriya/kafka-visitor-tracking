# Kafka Real-Time Website Visitor Tracking & Analytics

A real-time website visitor tracking and analytics system built using **Python, Flask, Apache Kafka, SQLite, HTML, CSS, JavaScript, and Docker**.

The project demonstrates a complete event-processing pipeline in which website visitor activity is captured as events, published to Apache Kafka, consumed by a Python application, stored in SQLite, and displayed through a web-based analytics dashboard.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Objectives](#objectives)
- [Key Features](#key-features)
- [System Architecture](#system-architecture)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [How the System Works](#how-the-system-works)
- [Visitor Identification](#visitor-identification)
- [Event Format](#event-format)
- [Kafka Configuration](#kafka-configuration)
- [Consumer and SQLite](#consumer-and-sqlite)
- [Analytics Dashboard](#analytics-dashboard)
- [Docker Deployment](#docker-deployment)
- [Running with Docker](#running-with-docker)
- [Testing the System](#testing-the-system)
- [Useful Docker Commands](#useful-docker-commands)
- [Verified Results](#verified-results)
- [Screenshots](#screenshots)
- [Data Persistence](#data-persistence)
- [Security and Privacy Notes](#security-and-privacy-notes)
- [Future Enhancements](#future-enhancements)
- [Learning Outcomes](#learning-outcomes)
- [Conclusion](#conclusion)

---

## Project Overview

The system tracks visitors who access a Flask-based website.

Whenever the website is opened or refreshed:

1. A unique visitor ID is generated for a new browser.
2. The visitor ID is stored in browser `localStorage`.
3. A `page_visit` event is created.
4. The event is sent to the Flask `/track` API.
5. The Flask producer publishes the event to Apache Kafka.
6. Kafka stores the event in the `website-visitors` topic.
7. A Python Kafka consumer receives the event.
8. The consumer stores the event in SQLite.
9. Visitor statistics are updated.
10. A separate Flask dashboard reads the SQLite database and displays analytics.

The project therefore demonstrates a complete **real-time event-processing pipeline**.

---

## Objectives

- Capture website visitor activity in real time.
- Generate a browser-specific visitor identifier.
- Send visitor events through a REST API.
- Use Apache Kafka as the event-streaming layer.
- Process Kafka events using Python.
- Store visitor events and summaries in SQLite.
- Display visitor analytics through a web dashboard.
- Containerize the application using Docker Compose.
- Demonstrate Kafka topic partitioning.
- Preserve SQLite data using a Docker named volume.

---

## Key Features

### Visitor Tracking
- Automatic visitor ID generation.
- Browser `localStorage` support.
- Page-visit event generation.
- Visitor event timestamping.

### Kafka Event Streaming
- Kafka topic: `website-visitors`.
- Topic configured with **3 partitions**.
- Internal Docker Kafka address: `kafka:29092`.
- External Kafka port: `9092`.

### Data Processing
- Python Kafka consumer.
- Individual event storage.
- Visitor summary updates.
- Visit-count tracking.

### Analytics Dashboard
The dashboard provides information such as:

- Total events.
- Unique visitors.
- Total visits.
- Today's visitors.
- Visitor summary.
- Page statistics.
- Recent visitor events.

### Docker
- Dockerfiles for producer, consumer, and dashboard.
- Docker Compose orchestration.
- ZooKeeper and Kafka containers.
- Kafka healthcheck.
- Persistent SQLite Docker volume.
- Internal and external Kafka listeners.

---

## System Architecture

### Application Flow

```text
                    ┌─────────────────────┐
                    │     Web Browser     │
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
                    │  website-visitors   │
                    │    3 Partitions     │
                    └──────────┬──────────┘
                               │
                               │ Consume Event
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
                    │     visitors.db     │
                    └──────────┬──────────┘
                               │
                               │ Read Analytics
                               ▼
                    ┌─────────────────────┐
                    │ Flask Dashboard     │
                    │      Port 5001      │
                    └─────────────────────┘
```

### Docker Architecture

```text
                         Docker Compose
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌───────────────┐     ┌───────────────┐     ┌────────────────┐
│  ZooKeeper    │     │     Kafka     │     │    Producer    │
│   :2181       │────▶│    :9092      │◀────│     :5000      │
└───────────────┘     │  :29092 int.  │     └───────┬────────┘
                      └───────┬───────┘             │
                              │                     │
                              ▼                     │
                      ┌───────────────┐             │
                      │    Consumer   │◀────────────┘
                      └───────┬───────┘
                              │
                              ▼
                      ┌───────────────┐
                      │ SQLite Volume │
                      │ visitor-data  │
                      └───────┬───────┘
                              │
                              ▼
                      ┌───────────────┐
                      │   Dashboard   │
                      │     :5001     │
                      └───────────────┘
```

---

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Application and data-processing logic |
| Flask | Producer API and analytics dashboard |
| Apache Kafka | Real-time event streaming |
| ZooKeeper | Kafka coordination for this deployment |
| kafka-python-ng | Python Kafka client |
| SQLite | Lightweight persistent database |
| HTML | Website structure |
| CSS | Website/dashboard styling |
| JavaScript | Visitor tracking and browser-side logic |
| Docker | Application containerization |
| Docker Compose | Multi-container orchestration |
| Git | Version control |
| GitHub | Source-code hosting |

---

## Project Structure

```text
kafka-visitor-tracking/
│
├── .dockerignore
├── .gitignore
├── README.md
├── docker-compose.yml
│
├── producer/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── consumer/
│   ├── consumer.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── visitors.db
│
├── dashboard/
│   ├── app.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── templates/
│       └── dashboard.html
│
└── docs/
    └── screenshots/
        ├── 01-kafka-topic.png
        ├── 02-producer-output.png
        ├── 03-consumer-output.png
        ├── 04-sqlite-output.png
        └── 05-dashboard.png
```

> Note: `consumer/visitors.db` is generated locally and is excluded from Git using `.gitignore`.

---

## How the System Works

### 1. Browser

The visitor opens the website at port 5000.

JavaScript checks whether a visitor ID already exists in `localStorage`.

### 2. Visitor Event

A page-visit event is generated containing information such as:

- Visitor ID
- Page
- IP address
- Timestamp
- Event type

### 3. Flask Producer

The browser sends the event to:

```text
POST /track
```

The Flask application validates the request and publishes the event to Kafka.

### 4. Kafka

Kafka receives the event in:

```text
website-visitors
```

The Docker deployment uses 3 partitions.

### 5. Consumer

The Python consumer subscribes to the Kafka topic and processes incoming events.

### 6. SQLite

The consumer stores event information and updates visitor-level statistics.

### 7. Dashboard

The dashboard reads the SQLite database and presents the processed information through a web interface.

---

## Visitor Identification

The browser stores the visitor ID in `localStorage`.

For new browsers, the application generates an identifier using a timestamp and random component.

Example format:

```text
visitor-mucfrqu6-bd32914kt
```

Returning to the website from the same browser can therefore continue using the stored visitor ID.

---

## Event Format

A visitor event follows this general JSON structure:

```json
{
  "visitor_id": "visitor-example-123",
  "page": "/",
  "ip_address": "127.0.0.1",
  "timestamp": "2026-09-22T10:00:00",
  "event": "page_visit"
}
```

The event is published to the Kafka topic:

```text
website-visitors
```

---

## Kafka Configuration

### Topic

```text
Topic: website-visitors
Partitions: 3
Replication Factor: 1
```

### Docker Kafka Addresses

Inside the Docker network:

```text
kafka:29092
```

From the host machine:

```text
localhost:9092
```

### Verify the Topic

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --describe \
  --topic website-visitors
```

Expected result includes:

```text
PartitionCount: 3
Partition: 0
Partition: 1
Partition: 2
```

---

## Consumer and SQLite

The consumer maintains two logical sets of information:

### Visitor Events

Stores individual page-visit events.

Fields include:

- Event ID
- Visitor ID
- Page
- IP address
- Timestamp
- Event type

### Visitor Summary

Stores visitor-level information such as:

- Visitor ID
- First visit
- Last visit
- Visit count

The database is used by the dashboard to calculate analytics.

---

## Analytics Dashboard

The dashboard is available at:

```text
http://localhost:5001
```

It displays:

### Summary Metrics

- **Total Events**
- **Unique Visitors**
- **Total Visits**
- **Today's Visitors**

### Visitor Analytics

- Visitor IDs
- First visit time
- Last visit time
- Visit counts

### Event Analytics

- Page statistics
- Recent visitor events

The dashboard provides a simple visual representation of the information processed by Kafka and stored in SQLite.

---

## Docker Deployment

The complete application can be deployed using Docker Compose.

### Docker Services

| Service | Container | Port | Purpose |
|---|---|---:|---|
| ZooKeeper | `visitor-zookeeper` | 2181 | Kafka coordination |
| Kafka | `visitor-kafka` | 9092 | Event streaming |
| Producer | `visitor-producer` | 5000 | Website and event producer |
| Consumer | `visitor-consumer` | — | Event processing and SQLite storage |
| Dashboard | `visitor-dashboard` | 5001 | Analytics dashboard |

### Docker Images

- `confluentinc/cp-zookeeper:7.7.7`
- `confluentinc/cp-kafka:7.7.7`
- Python 3.11 slim for application containers

### Kafka Readiness

The Kafka service includes a Docker healthcheck. Producer and consumer services wait for Kafka to become healthy before starting.

This avoids the startup race condition that can occur when an application attempts to connect to Kafka before the broker is ready.

---

## Running with Docker

### Prerequisites

Install:

- Docker
- Docker Compose

Verify:

```bash
docker --version
docker compose version
```

### Build the Images

From the project directory:

```bash
docker compose build
```

### Start the System

```bash
docker compose up -d
```

### Check Containers

```bash
docker compose ps
```

Expected services:

```text
visitor-zookeeper
visitor-kafka
visitor-producer
visitor-consumer
visitor-dashboard
```

Kafka should report a healthy status.

### Open the Website

```text
http://localhost:5000
```

### Open the Dashboard

```text
http://localhost:5001
```

### Stop the System

```bash
docker compose down
```

> Avoid `docker compose down -v` if the SQLite Docker volume needs to be preserved.

---

## Testing the System

### Test 1: Open the Website

Open:

```text
http://localhost:5000
```

The website should record a visitor event.

### Test 2: Refresh the Website

Refresh the page once or more.

Each page load sends a visitor event.

### Test 3: Open the Dashboard

Open:

```text
http://localhost:5001
```

The dashboard should reflect the processed events.

### Test 4: Inspect Kafka

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --describe \
  --topic website-visitors
```

### Test 5: Inspect Consumer Logs

```bash
docker compose logs --tail=50 consumer
```

### Test 6: Inspect Producer Logs

```bash
docker compose logs --tail=50 producer
```

### Test 7: Check All Services

```bash
docker compose ps
```

---

## Useful Docker Commands

### View All Logs

```bash
docker compose logs -f
```

### View Kafka Logs

```bash
docker compose logs -f kafka
```

### View Producer Logs

```bash
docker compose logs -f producer
```

### View Consumer Logs

```bash
docker compose logs -f consumer
```

### View Dashboard Logs

```bash
docker compose logs -f dashboard
```

### Rebuild Application Images

```bash
docker compose build
```

### Restart Services

```bash
docker compose restart
```

### Check Kafka Topic

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list
```

### Describe the Visitor Topic

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --describe \
  --topic website-visitors
```

---

## Verified Results

The Docker deployment was tested end-to-end.

### Verified Application Flow

```text
Browser
  ↓
Flask Producer
  ↓
Kafka
  ↓
Python Consumer
  ↓
SQLite
  ↓
Flask Dashboard
```

### Verified Dashboard

The tested dashboard successfully displayed:

- Total events
- Unique visitors
- Total visits
- Today's visitors
- Visitor summary
- Visit timestamps

### Verified Kafka Topic

The final Kafka topic configuration was verified as:

```text
Topic: website-visitors
PartitionCount: 3
ReplicationFactor: 1
```

Partitions:

```text
Partition 0
Partition 1
Partition 2
```

### Verified Docker Services

The final Docker test showed all five services running:

```text
visitor-zookeeper   Up
visitor-kafka       Up (healthy)
visitor-producer    Up
visitor-consumer    Up
visitor-dashboard   Up
```

---

## Screenshots

The repository contains screenshots demonstrating the main stages of the project.

| Screenshot | Description |
|---|---|
| `01-kafka-topic.png` | Kafka topic configuration |
| `02-producer-output.png` | Producer event output |
| `03-consumer-output.png` | Consumer processing output |
| `04-sqlite-output.png` | SQLite visitor data |
| `05-dashboard.png` | Analytics dashboard |

The screenshots are stored in:

```text
docs/screenshots/
```

---

## Data Persistence

The Docker deployment uses a named volume:

```text
visitor-data
```

The volume is shared between the consumer and dashboard containers.

The consumer stores the SQLite database inside this persistent volume, while the dashboard reads the same database.

This allows the application containers to be recreated without automatically removing the stored visitor data.

---

## Security and Privacy Notes

This project is intended as an educational demonstration.

Consider the following before using it in a production environment:

- Avoid exposing development Flask servers directly to the public internet.
- Protect APIs with authentication and authorization where required.
- Apply appropriate input validation.
- Consider privacy requirements before collecting IP addresses or visitor identifiers.
- Use HTTPS for production deployments.
- Store secrets outside source code.
- Apply appropriate Kafka authentication and encryption for production systems.
- Review database access permissions.

---

## Future Enhancements

Possible extensions include:

1. Replace SQLite with PostgreSQL for larger workloads.
2. Add authentication to the analytics dashboard.
3. Add charts for visits over time.
4. Add geographic analytics where legally and technically appropriate.
5. Add multiple tracked pages.
6. Add Kafka consumer groups for scalable processing.
7. Add Kafka retention and production-oriented configuration.
8. Add Redis caching for frequently requested dashboard statistics.
9. Add Docker healthchecks for additional application services.
10. Add monitoring using Prometheus and Grafana.
11. Add automated testing.
12. Add CI/CD using GitHub Actions.
13. Add production deployment configuration.
14. Add export functionality for analytics data.

---

## Learning Outcomes

This project provides practical experience with:

- Event-driven architecture.
- Real-time data pipelines.
- Apache Kafka producers and consumers.
- Kafka topics and partitions.
- REST APIs using Flask.
- Browser-side JavaScript.
- Browser `localStorage`.
- JSON event structures.
- SQLite database operations.
- Data aggregation.
- Docker containerization.
- Docker Compose orchestration.
- Service networking.
- Persistent Docker volumes.
- Git and GitHub version control.

---

## Conclusion

The Kafka Real-Time Website Visitor Tracking & Analytics project demonstrates how a web application can generate real-time events and process them through a streaming pipeline.

The final system combines:

```text
Web Development
       +
REST API
       +
Apache Kafka
       +
Python Event Processing
       +
SQLite
       +
Analytics Dashboard
       +
Docker
```

The project provides a practical demonstration of a complete real-time visitor analytics workflow from browser activity to processed dashboard results.

---

## Author

**Kafka Real-Time Website Visitor Tracking & Analytics**

Repository:

https://github.com/chinthavishnupriya/kafka-visitor-tracking
