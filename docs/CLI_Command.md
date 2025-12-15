# CLI_Command.md

## Project Setup
```bash
# 1. Remove any existing virtual environment (if present)
rm -rf venv

# 2. Create a Python 3.12 virtual environment
python3.12 -m venv venv
source venv/bin/activate

# 3. Install Python dependencies
pip install -r requirements.txt
```

## Data Generation
```bash
# Generate synthetic click‑stream data
python generate_data.py
```

## Run Spark Pipeline
```bash
# Execute the full Spark pipeline (Bronze → Silver → Gold)
python spark_pipeline.py
# Results are written to `output/` and `delta/` directories.
```

## View Results
```bash
# List generated CSV/Parquet files
ls output/

# Inspect a CSV file (example)
head -n 20 output/gold_report.csv
```

## Compile Walkthrough PDF (LaTeX)
```bash
# Compile the walkthrough LaTeX source into a PDF
pdflatex Walkthrough.tex
# Open the PDF
open Walkthrough.pdf
```
