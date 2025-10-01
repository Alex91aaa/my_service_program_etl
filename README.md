# my_service_program_etl
--- Anonymized and no config -> not runnable! 

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
