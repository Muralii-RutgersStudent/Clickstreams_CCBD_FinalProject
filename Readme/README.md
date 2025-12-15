# README.md (updated)

# CCBD Project – Clickstream Analytics

**Date:** December 5, 2025

---

## Overview
This project simulates click‑stream data for an e‑commerce site and processes it with **PySpark** and **Delta Lake**. The pipeline consists of three layers (Bronze → Silver → Gold) and produces session‑level and funnel analytics.

## Current Progress
- Data generation (`generate_data.py`) – ✅
- Spark pipeline (`spark_pipeline.py` / `pipeline.py`) – core structure in place ✅
- Visualization utilities (`visualize.py`) – ✅
- Documentation (`README.md`, LaTeX status report) – ✅

## **⚠️ Pending Spark Work**
> The Spark component is still under development. See the `task.md` checklist for details.

---

## Quick‑Start Guide
### Prerequisites
- **Java 11** (required by Spark)
- **Python 3.12**
- Install dependencies:
```bash
pip install -r requirements.txt
```
- Ensure the `spark-submit` command is on your PATH (comes with Spark distribution).

### Running Locally
```bash
# 1. Generate synthetic data
python generate_data.py

# 2. Execute the Spark pipeline
python spark_pipeline.py
```
The pipeline will create Delta tables under `delta/bronze`, `delta/silver`, and `delta/gold`, and export a CSV/Parquet report to `output/`.

---

## Spark Configuration Details
| Setting | Recommended Value |
|---------|-------------------|
| `spark.executor.memory` | `2g` |
| `spark.driver.memory`   | `2g` |
| `spark.sql.shuffle.partitions` | `200` |
| Delta extensions | `io.delta.sql.DeltaSparkSessionExtension` |
| Delta catalog | `org.apache.spark.sql.delta.catalog.DeltaCatalog` |

You can override these in the `spark-submit` command or by editing the builder configuration in `spark_pipeline.py`.

---

## Environment Reproduction
1. Install Java 11:
```bash
# macOS example
brew install openjdk@11
```
2. Create a virtual environment with Python 3.12 and install requirements:
```bash
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
3. Verify Spark installation:
```bash
spark-shell --version
```

---

## Future Scalability (Optional)
- **ML integration**: Outline steps to train a model on the Gold layer and serve predictions via Spark ML pipelines.

---

*The project is ongoing; many higher‑level components remain under active development.*
