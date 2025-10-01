# my_service_program_etl
⚠️ Note: This repository is anonymized for portfolio purposes. Config files and credentials are not included. The code is not runnable as-is.

Automated data pipeline for event-based systems.  

## Overview
- Collects event, entity, and transaction data.
- Cleans and integrates data into a PostgreSQL database.
- Processes historical and real-time data.
- Supports multithreading and automated logging.

## Tech Stack
- Python 3.x
- PostgreSQL
- Pandas, Requests, JSON
- Threading, ETL, Logging

## Features / Highlights
- ETL pipeline with historical & daily data processing
- Threaded real-time processing of events
- Automated foreign key resolution and bulk DB integration
- Logging, retry mechanisms, error handling


🔹 Technical Skills Demonstrated

ETL Automation: End-to-end workflow for structured data pipelines.
APIs & Web Data: Resilient API interactions with retries, logging, and error handling.
Python Engineering: Modular, multi-file project with utilities and shared functions.
Databases: Bulk loading, resolving foreign keys, transaction handling.
Concurrency: Multi-threaded processing of real-time data.
DevOps Practices: Logging, configuration management, and separation of concerns.


🔹 Challenges & Solutions

Challenge: APIs occasionally failed or returned incomplete data.
✅ Solution: Implemented a retry decorator with customizable retries and delays.
Challenge: Large volumes of historical data needed efficient integration.
✅ Solution: Used bulk inserts and smart date-based batch processing.
Challenge: Different datasets (events, entities, return rates) required synchronization.
✅ Solution: Created a schema dictionary and resolver functions to keep relationships consistent.
Challenge: Need for real-time feedback and monitoring.
✅ Solution: Built a Discord bot integration for live alerts and summaries.

## How It Works

1. **Data Fetch**
   - Fetch base data, entity info, and return rates
   - Generate event lists for processing

2. **Data Cleaning**
   - Prepare sessions, events, transactions
   - Normalize transaction values

3. **Data Integration**
   - Insert cleaned data into database
   - Resolve foreign keys for entity relationships

4. **Real-time Processing**
   - Threads handle events concurrently
   - Output daily CSVs and update return rates

5. **Notifications**
   - Send important results and summaries via Discord bot

---

## Module Descriptions

| Module | Purpose |
|--------|---------|
| `main.py` | Orchestrates the whole ETL and real-time process |
| `utils.py` | Helper functions: YAML loader, logging, retries, history generation |
| `fetcher.py` | Fetches base, entity, and return rate data |
| `cleaner.py` | Data cleaning, normalization, transaction value preparation |
| `data_integration.py` | Inserts data into database, resolves relationships |
| `threads.py` | Threaded processing for live events |
| `etl_additional_script.py` | Extra ETL routines for historical data |
| `discord_bot.py` | Sends notifications to Discord |
| `loader.py` | Utility for loading daily data |

---

```mermaid
flowchart TD
    A[Base Data Fetch - fetcher.py] --> B[Event List Generation - fetcher.py]
    B --> C[Entity / Transaction / Return Rate Data Fetch - fetcher.py]
    C --> D[Data Cleaning - cleaner.py]
    D --> E[Data Integration / DB - loader.py, etl_additional_script.py]
    E --> F[Foreign Key Resolver - loader.py]
    F --> G[Threaded Real-time Processing - threads.py]
    G --> H[Daily CSV / Return Rate Output - etl_additional_script.py]
    H --> I[Bot / Discord Notifications - discord_bot.py]
    
    subgraph utils
        U[General Utilities - utils.py]
    end
    
    D --> U
    E --> U
