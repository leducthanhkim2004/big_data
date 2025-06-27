# Big Data Spark Project

This project contains PySpark code for analyzing IMDB dataset using Apache Spark.

## Requirements

- Python 3.10+
- Apache Spark
- Hadoop (for HDFS)

## Setup Instructions

### Option 1: Using Conda (Recommended)

1. Install Miniconda or Anaconda
2. Create environment from the exported file:
   ```bash
   conda env create -f environment.yml
   conda activate spark-env
   ```

### Option 2: Using pip

1. Create a virtual environment:
   ```bash
   python -m venv spark_env
   # On Windows:
   spark_env\Scripts\activate
   # On macOS/Linux:
   source spark_env/bin/activate
   ```

2. Install requirements:
   ```bash
   pip install -r requirements.txt
   ```

### Option 3: Minimal Setup (Just Spark essentials)

```bash
pip install -r spark_requirements.txt
```

## Files

- `test_spark.py` - Main Spark application for IMDB dataset analysis
- `environment.yml` - Complete conda environment specification
- `requirements.txt` - Complete pip requirements
- `spark_requirements.txt` - Minimal requirements for Spark functionality

## Usage

Make sure Hadoop and Spark are running, then execute:

```bash
python test_spark.py
```

## Notes

- Ensure HDFS is running on `localhost:9000`
- The dataset should be located at `hdfs://localhost:9000/test/data/IMDB-Dataset.csv`
