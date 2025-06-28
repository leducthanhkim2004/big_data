# IMDB Dataset EDA with Apache Spark

This project performs comprehensive Exploratory Data Analysis (EDA) on the IMDB movie reviews dataset using Apache Spark.

## Features

The EDA script provides:

1. **Basic Dataset Information** - Shape, column names, data types
2. **Schema Analysis** - Data structure and field types
3. **Missing Values Analysis** - Null/empty value detection and percentages
4. **Descriptive Statistics** - Mean, std, min, max for numerical columns
5. **Unique Values Count** - Cardinality of each column
6. **Sentiment Distribution** - Breakdown of positive/negative reviews
7. **Text Length Analysis** - Character and word count statistics
8. **Sample Records Analysis** - Examples of positive and negative reviews
9. **Data Quality Checks** - Duplicate detection
10. **Performance Information** - Partitioning and execution plan
11. **Results Export** - Save statistics to CSV files

## Prerequisites

- Apache Spark installed and configured
- Hadoop HDFS running (if using HDFS storage)
- Python environment with required packages

## Installation

### Option 1: Using Conda Environment

```bash
# Create new environment from environment.yml
conda env create -f environment.yml
conda activate big_data
```

### Option 2: Using pip

```bash
# Install requirements
pip install -r spark_requirements.txt
```

## Dataset Setup

1. Ensure your IMDB dataset is uploaded to HDFS:
```bash
# Copy dataset to HDFS
hdfs dfs -put IMDB-Dataset.csv /test/data/
```

2. Verify the file exists:
```bash
hdfs dfs -ls /test/data/
```

## Usage

### Running the EDA Script

```bash
python test_spark.py
```

### Expected Output

The script will generate:
- Comprehensive console output with all analysis results
- `summary_statistics/` directory with descriptive statistics CSV
- `sentiment_distribution/` directory with sentiment breakdown CSV

### Sample Output Sections

```
================================================================================
IMDB DATASET EXPLORATORY DATA ANALYSIS
================================================================================

==================================================
1. BASIC DATASET INFORMATION
==================================================
Dataset Shape: 50000 rows × 2 columns

Column Names:
  1. review
  2. sentiment

==================================================
2. SCHEMA ANALYSIS
==================================================
root
 |-- review: string (nullable = true)
 |-- sentiment: string (nullable = true)
```

## Configuration

### Spark Configuration

The script uses optimized Spark settings:
- Adaptive Query Execution enabled
- Partition coalescing enabled
- DataFrame caching for performance

### HDFS Path Configuration

Update the HDFS path in the script if needed:
```python
hdfs_path = "hdfs://localhost:9000/test/data/IMDB-Dataset.csv"
```

## Troubleshooting

### Common Issues

1. **HDFS Connection Error**
   - Ensure Hadoop is running: `start-dfs.sh`
   - Check HDFS namenode: `http://localhost:9870`

2. **File Not Found**
   - Verify file exists: `hdfs dfs -ls /test/data/`
   - Check file permissions

3. **Out of Memory**
   - Increase Spark driver memory: `.config("spark.driver.memory", "4g")`
   - Reduce dataset size for testing

4. **Missing Dependencies**
   - Install packages: `pip install -r spark_requirements.txt`
   - For Conda: `conda install package-name`

## Performance Optimization

- Dataset is cached after loading for multiple operations
- Adaptive query execution optimizes joins and aggregations
- Results are coalesced to single partition for export

## Output Files

The script generates:
- **summary_statistics/**: Basic descriptive statistics
- **sentiment_distribution/**: Sentiment breakdown counts

## Extension Ideas

1. Add visualization plots (saved as images)
2. Implement text preprocessing and cleaning
3. Add word frequency analysis
4. Create correlation analysis for numerical features
5. Add more advanced text analytics (TF-IDF, n-grams)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with your dataset
5. Submit a pull request
