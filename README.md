# service_program_etl

## Portfolio Context
⚠️ This repository is anonymized for portfolio purposes:  
- Config files and credentials are removed  
- Code is not runnable as-is  
- Designed to demonstrate pipeline architecture, modular code, and technical skills
Automated data pipeline for event-based systems.  

## Overview

Full ETL and real-time processing pipeline for complex event-based data.  
Handles fetching, cleaning, integrating, and processing historical and live data.  

**Key features:**
- Automates ETL pipeline: fetches, cleans, transforms, and integrates event-based data into PostgreSQL
- Supports multithreaded real-time processing
- Integrates historical data efficiently with retry and error handling mechanisms
- Resolves entity relationships and foreign keys automatically
- Detailed logging and monitoring of all processes
- CSV export and Discord notifications (in development)

---

## Tech Stack
- Python 3.x
- PostgreSQL
- Pandas, Requests, JSON
- Threading, ETL, Logging

🔹 Technical Highlights
- **ETL Automation:** End-to-end workflow for structured data pipelines
- **APIs & Web Data:** Robust fetching with retries and logging
- **Python Engineering:** Modular, multi-file architecture with reusable utilities
- **Databases:** Bulk loading, foreign key resolution, transaction handling
- **Concurrency:** Multi-threaded real-time event processing
- **DevOps Practices:** Logging, config management, and separation of concerns


🔹 Challenges & Solutions
- **Unstable API data:** APIs occasionally failed or returned incomplete data  
✅ Solution: Retry decorator with customizable retries and delays

- **Large historical dataset integration:** Needed efficient batch processing  
✅ Solution: Bulk inserts and smart date-based processing

- **Synchronizing multiple datasets:** Events, entities, return rates required consistent relationships  
✅ Solution: Schema dictionary and resolver functions

- **Real-time feedback:** Needed alerts and summaries  
✅ Solution: Discord bot integration (currently under development)

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
   - Send important results and summaries via Discord bot (currently under development)

---

## Future work / In development.

Machine Learning / Deep Learning (predictor.py)

- Purpose: Generate probabilistic predictions for events and outcomes to enhance automated analysis.
- Architecture & Techniques:
   - DQN Agent: Deep Q-Network for reinforcement learning-based prediction of sequential events.
   - Multi-head Classification: Predict multiple types of outcomes simultaneously (e.g., event result, transaction trend, entity behavior).
   - Attention Mechanisms: Improve focus on key features and historical event dependencies.
   - Reward Shaping: Uses historical outcomes and transaction values to optimize predictive policy.
- Data Flow: Consumes cleaned and integrated data from ETL pipeline (cleaner.py, loader.py) and outputs probability scores for upcoming events.
- Integration: Predictions can feed into notifications or analytics dashboards for real-time decision support.

Benefits:

- Demonstrates advanced reinforcement learning and multi-task prediction.
- Directly integrates with your ETL pipeline for real-world use cases.
- Highlights your ability to implement complex ML/DL architectures in production workflows.
  
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

    %% Highlight ML module
    G --> J{{Probabilistic Predictions - predictor.py ML/DL}}

    subgraph utils
        U[General Utilities - utils.py]
    end

    J --> E
    D --> J
    J --> I
    D --> U
    E --> U

    %% Styling for emphasis
    style J fill:#ffcc00,stroke:#333,stroke-width:2px
    style A fill:#f9f,stroke:#333,stroke-width:2px
    style I fill:#ff9,stroke:#333,stroke-width:2px
