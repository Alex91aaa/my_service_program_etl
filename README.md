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

flowchart TD
    A[Base Data Fetch] --> B[Event List Generation]
    B --> C[Entity / Transaction / Return Rate Data Fetch]
    C --> D[Data Cleaning]
    D --> E[Data Integration / DB]
    E --> F[Foreign Key Resolver]
    F --> G[Threaded Real-time Processing]
    G --> H[Daily CSV / Return Rate Output]
    H --> I[Bot / Discord Notifications]
