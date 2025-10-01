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

```mermaid
flowchart TD
    A[Base Data Fetch\n(fetcher.py)] --> B[Event List Generation\n(fetcher.py)]
    B --> C[Entity / Transaction / Return Rate Data Fetch\n(fetcher.py)]
    C --> D[Data Cleaning\n(cleaner.py)]
    D --> E[Data Integration / DB\n(loader.py, etl_additional_script.py)]
    E --> F[Foreign Key Resolver\n(loader.py)]
    F --> G[Threaded Real-time Processing\n(threads.py)]
    G --> H[Daily CSV / Return Rate Output\n(etl_additional_script.py)]
    H --> I[Bot / Discord Notifications\n(discord_bot.py)]
    subgraph utils
        U[General Utilities\n(utils.py)]
    end
    D --> U
    E --> U
