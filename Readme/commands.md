# Project Commands & Execution Guide

This document organizes the commands required to run the Clickstream Analytics project in two environments: **Local** and **Docker Container**.

---

## 1. Local Execution
Run the project directly on your machine (requires Python 3.12+ and Java 11+).

### Setup
Install dependencies:
```bash
pip install -r requirements.txt
```

### Execution steps
1. **Generate Data**:
   ```bash
   python src/generate_data.py
   ```
2. **Run Pipeline** (Ingest, Process, Visualize):
   ```bash
   python src/spark_pipeline.py
   ```

### Outputs
- **Reports**: `output/gold_report.csv`, `output/gold_report.parquet`
- **Visualizations**: `funnel_chart.png`, `session_duration.png`
- **Delta Tables**: `delta/bronze`, `delta/silver`, `delta/gold`

---

## 2. Docker Execution
Run the project inside your existing `docker-spark` container (`docker-spark-master-1`).

> **Note**: These commands assume the container is running and accessible. They set up a compatible Python environment (Miniconda) inside the container because the default system Python (3.5) is too old for PySpark 3.4+.

### Step A: One-time Container Setup
Prepare the container by installing Miniconda and project dependencies.

```bash
# 1. Create project directory in container
docker exec docker-spark-master-1 mkdir -p /tmp/ccbd_project

# 2. Install Miniconda (Python 3.8) - required for modern PySpark
docker exec docker-spark-master-1 curl -o /tmp/miniconda_old.sh https://repo.anaconda.com/miniconda/Miniconda3-py38_4.9.2-Linux-x86_64.sh
docker exec docker-spark-master-1 bash /tmp/miniconda_old.sh -b -p /tmp/miniconda

# 3. Copy requirements and install
docker cp requirements.txt docker-spark-master-1:/tmp/ccbd_project/requirements.txt
docker exec docker-spark-master-1 /tmp/miniconda/bin/pip install -r /tmp/ccbd_project/requirements.txt
```

### Step B: Execution Cycle
Run these commands for each new execution of the code.

```bash
# 1. Copy latest code and data to container
docker cp src/spark_pipeline.py docker-spark-master-1:/tmp/ccbd_project/spark_pipeline.py
docker cp src/generate_data.py docker-spark-master-1:/tmp/ccbd_project/generate_data.py
docker cp clickstream_data.json docker-spark-master-1:/tmp/ccbd_project/clickstream_data.json

# 2. Execute Pipeline inside container
# We unset conflicting environment variables to ensure the local Spark session starts correctly.
docker exec -w /tmp/ccbd_project docker-spark-master-1 sh -c "unset SPARK_HOME HADOOP_CONF_DIR SPARK_CONF_DIR MASTER SPARK_DIST_CLASSPATH; /tmp/miniconda/bin/python spark_pipeline.py"
```

### Step C: Retrieve Results
Copy the generated outputs back to your host machine.

```bash
mkdir -p output_docker

# Copy Output Data
docker cp docker-spark-master-1:/output/ ./data/output_docker/

# Copy Visualizations
docker cp docker-spark-master-1:/tmp/ccbd_project/funnel_chart.png ./funnel_chart_docker.png
docker cp docker-spark-master-1:/tmp/ccbd_project/session_duration.png ./session_duration_docker.png
```

---

## 3. Project File Organization
- **`src/`**: Contains the source code (`generate_data.py`, `spark_pipeline.py`, `visualize.py`).
- **`data/`**: Stores all data files:
    - `clickstream_data.json`: Raw input data.
    - `delta/`: Delta Lake tables (Bronze, Silver, Gold).
    - `output/`: CSV/Parquet reports from local execution.
    - `output_docker/`: CSV/Parquet reports from Docker execution.
    - `*.png`: Visualization charts.
- **`docs/`**: Documentation files (`Walkthrough.pdf`, `Readme1.md`, etc.).
- **`config/`**: Configuration files (`requirements.txt`).
- **`logs/`**: Log files.
- **`legacy/`**: Contains older or deprecated code (e.g., `pipeline.py`).
- **`Dockerfile`**: Docker configuration.
