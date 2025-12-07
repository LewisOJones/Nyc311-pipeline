## NYC 311 Data Pipeline – ETL + Analysis Assignment

A modular Python data pipeline that ingests NYC 311 Service Requests from the NYC Open Data API, validates and normalizes the data, persists it locally, and provides lightweight analytics and alerting functionality.

This project was designed to demonstrate:

* Clean ETL architecture

* Schema-driven validation

* Incremental ingestion - using unique keys

CLI-based orchestration

### <b><u>Project Overview</b></u>
I have implemented a standard E-T-L design where: 

<b>E</b>xtract

* Polls the NYC 311 API (one-off or continuous mode)

* Supports incremental ingestion using a timestamp checkpoint

* Handles rate limits with retry + backoff

<b>T</b>ransform

* Validates incoming records using a dataclass (NYC311Record)

* Parses timestamps

* Normalizes latitude/longitude

* Drops malformed or incomplete rows

<b>L</b>oad

* Persists cleaned data into SQLite

* Enforces unique index on unique_key

* Uses INSERT OR IGNORE to avoid duplicates

<b><u>Downstream Analytics</b></u>

* Historical trend visualisations

* Alerting for recent complaints matching user filters

<b><u>CLI Interface</b></u>

The entire project is orchestrated via a clean command-line interface using argparse.

### <b><u>2. Architecture & Design Choices</b></u>
<b>Reader Layer</b>

* Uses requests.Session for efficient repeated queries

* Supports an optional "since timestamp" for incremental ingestion

* Includes exception handling for 429 rate limits

* Optional API token support (taken from NYc 311 developer notes)

* Extension could be to add caching for testing efficiency

<b>Schema Layer</b>

* NYC311Record dataclass validates required fields (unique_key, created_date, complaint_type)

* Parses timestamps with dateutil

* Safely casts lat/lon to floats (albeit, currently not used in analytics)

* Ensures only relevant fields move into storage

<b>Writer Layer</b> 

* SQLite-based local data store

* Automatically creates the table based on DataFrame schema

* Enforces a unique index

* Uses INSERT OR IGNORE for safe upsert behavior

* Reports rows inserted

* abstracted to allow for other types of writer e.g. parquet files

<b>Runner (ETL Orchestration)</b>

* Independently coordinates Reader → Schema → Writer

* Decoupled from implementation details

* Makes ETL testable and extendable (can mock components, plug in other strategies etc)