# Walkthrough

## Project Summary (Group 7)

- **Data Generation** (`generate_data.py`) – synthetic click‑stream JSON created.
-## 3. Spark Pipeline Execution
The Spark pipeline (`spark_pipeline.py`) was successfully executed locally. It processed the synthetic clickstream data through three layers:

### Bronze Layer (Raw Ingestion)
- **Status**: Successfully ingested raw JSON data.
- **Validation**: Schema validation applied; malformed records isolated.
- **Output**: Delta table at `delta/bronze`.

### Silver Layer (Sessionization)
- **Status**: Successfully transformed raw events into user sessions.
- **Logic**: Applied 30-minute timeout logic.
- **Output**: Delta table at `delta/silver`.

### Gold Layer (Analytics & Reporting)
- **Status**: Aggregated session metrics and funnel analysis.
- **Metrics**: Session duration, page views, conversion rates.
- **Output**: 
    - Delta table at `delta/gold`.
    - CSV report at `output/gold_report.csv`.
    - Visualizations: `funnel_chart.png` and `session_duration.png`.

## 4. Visualizations
### Conversion Funnel
![Conversion Funnel](funnel_chart.png)

### Session Duration Distribution
![Session Duration](session_duration.png)

## 5. Conclusion
The project successfully demonstrates a local-only PySpark and Delta Lake pipeline, handling data ingestion, transformation, and advanced analytics without cloud dependencies.
- **Spark Pipeline** (`spark_pipeline.py` / `pipeline.py`) – Bronze → Silver → Gold layers implemented with Delta Lake.
- **Visualization** (`visualize.py`) – basic plots for data inspection.
- **Documentation** – README updated with Spark configuration, quick‑start guide, and environment setup.
- **Task Checklist** (`task.md`) – all items marked as completed.
- **LaTeX Walkthrough** (`Walkthrough.tex`) – source for a PDF walkthrough.

## Completed Work
The pipeline now includes:
- Robust schema validation and error handling for raw JSON.
- Refined session‑timeout logic and persisted intermediate session metrics.
- Additional funnel stages (`payment_failed`, `order_placed`).
- Conversion‑rate calculations by device, browser, and time‑of‑day.
- Export of analytics results to CSV and Parquet.
- Unit and integration tests for Spark transformations.

## How to Run
1. Generate data: `python generate_data.py`
2. Execute the Spark pipeline: `python spark_pipeline.py`
3. Results are written to the `output/` directory and Delta tables under `delta/`.

---

*The project is ongoing; many higher‑level components remain under active development.*
