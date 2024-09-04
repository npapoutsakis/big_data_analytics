# Spark Analytics Project: Jaccard Index Calculation

## Overview

This project is a Scala-based implementation for calculating the Jaccard Index over a large dataset using Apache Spark and Hadoop. The Jaccard Index is a statistical measure used to evaluate the similarity between two sets. This implementation leverages Spark's distributed computing capabilities to efficiently process large datasets and compute the Jaccard Index.

## Project Details

### Code Description

The provided Scala code performs the following tasks:

1. **Read Data:**
   - Reads data from Hadoop HDFS:
     - `category_file` - Contains document IDs and their associated categories.
     - `terms_file` - Contains document IDs and the terms present in each document.
     - `stems_file` - Maps term IDs to their respective stems.

2. **Data Processing:**
   - **STEMS**: Maps term IDs to stems.
   - **CATEGORIES**: Maps document IDs to categories.
   - **TERMS**: Maps document IDs to terms and flattens them for processing.
   - **COGROUP**: Co-groups categories and terms based on document IDs.
   - **INTERSECTION**: Calculates the intersection between categories and terms for each document.
   - **JOIN**: Joins the intersections with category counts and term counts to compute the Jaccard Index.

3. **Calculate Jaccard Index:**
   - The Jaccard Index is computed for each term-category pair using the formula:
     \[
     Jaccard Index = \frac{\text{Number of documents containing both term and category}}{\text{Number of documents containing either term or category}}
     \]

\[
Jaccard\ Index = \frac{\text{Number of documents containing both term and category}}{\text{Number of documents containing either term or category}}
\]

4. **Output:**
   - The results are saved back to Hadoop HDFS in a specified directory.

### Dependencies

- **Java JDK 8 or higher**
- **Apache Spark**: Compatible version with your Hadoop setup.
- **Hadoop**: Version compatible with your Spark setup.
- **SBT (Scala Build Tool)**: For building and running the Scala code.

### Configuration

- **Spark Configuration**: 
  - Master node is set to `local[*]` for local execution.
  - Adjust Spark configurations (e.g., memory and number of executors) based on your cluster specifications.

- **Hadoop Configuration**:
  - Ensure Hadoop is set up correctly and that paths to HDFS files are accurate.

### Running the Code

1. **Build the Project**:
   - Compile the Scala code and create a JAR file using SBT.

2. **Submit the Spark Job**:
   - Use `spark-submit` to run the application. Ensure to adjust paths and settings as required:
     ```bash
     spark-submit \
       --class SparkAnalytics \
       --master local[*] \
       path/to/your/jarfile.jar
     ```

3. **Verify Output**:
   - The output will be saved to HDFS in the directory specified by `output`:
     - Example: `hdfs://localhost:9000/reuters/JaccardIndex`

