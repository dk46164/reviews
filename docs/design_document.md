# Restaurant Review Aggregation System Design

## 1. System Overview

This document outlines the design of a scalable, fault-tolerant batch processing system for aggregating restaurant reviews. The system uses Dask for distributed computing on EKS, S3 for data lake storage, and Snowflake for data warehousing with Type 2 tables to track historical changes.

## 2. Architecture Components

- **Compute**: Dask cluster on EKS
- **Storage**:
  - Amazon S3 for raw data and intermediate results
  - Snowflake for aggregated data storage
- **Monitoring and Logging**: Amazon CloudWatch

## 3. Data Flow

### 3.1 Input
- Reviews JSONL files in S3 bucket (s3://reviews-input/)
- Inappropriate words file in S3 bucket (s3://config/inappropriate_words.txt)

### 3.2 Processing
- Dask cluster reads and processes data
- Filtering and aggregation occur at the restaurant level

### 3.3 Output
- Aggregated results loaded into Snowflake (SCD Type 2) table

## 4. Processing Logic Flow

1. **Data Ingestion**
   - Read review files from S3 using `dask.dataframe`
   - Validate input data against JSON schema
   - Filter out invalid records

2. **Data Processing**
   - Filter out outdated reviews (> 3 years old)
   - Replace inappropriate words with asterisks
   - Filter out reviews with > 20% inappropriate words
   - Aggregate data at the restaurant level:
     - Number of reviews
     - Average score
     - Average length of review
     - Review age (oldest, newest, average)

3. **Data Output**
   - Prepare aggregated data for Snowflake Type 2 table insertion
   - Load data into Snowflake using COPY command or Snowpipe

## 5. Snowflake Table Structure

### 5.1 Stage Table

Table Name: RestaurantReviews_Stage

| Column Name          | Data Type | Description                                 |
|----------------------|-----------|---------------------------------------------|
| Restaurant_ID        | STRING    | Natural Key                                 |
| Review_ID            | STRING    | Unique identifier for each review           |
| Review_Text          | STRING    | Text of the review                          |
| Review_Score         | FLOAT     | Score given in the review                   |
| Review_Length        | INTEGER   | Length of the review text                   |
| Review_Date          | TIMESTAMP | Date of the review                          |
| Processed_Timestamp  | TIMESTAMP | Timestamp of when the review was processed  |
| Record_Source        | STRING    | Source file name                            |


### 5.2 Aggregation and Reporting Table

Table Name: RestaurantAggregations_Type2

| Column Name           | Data Type | Description                                   |
|-----------------------|-----------|-----------------------------------------------|
| Restaurant_ID         | STRING    | Natural Key                                   |
| Number_of_Reviews     | INTEGER   | Total number of reviews for the restaurant    |
| Average_Score         | FLOAT     | Average score of all reviews                  |
| Average_Review_Length | INTEGER   | Average length of reviews                     |
| Oldest_Review_Age     | INTEGER   | Age of the oldest review in days              |
| Newest_Review_Age     | INTEGER   | Age of the newest review in days              |
| Average_Review_Age    | FLOAT     | Average age of all reviews in days            |
| Effective_From        | TIMESTAMP | Start date of this version                    |
| Effective_To          | TIMESTAMP | End date of this version (NULL if current)    |
| Is_Current            | BOOLEAN   | Flag indicating if this is the current version|

## 6. Fault Tolerance and Scalability

- Utilize Dask's built-in fault tolerance mechanisms
- Implement auto-scaling for the Dask cluster using AWS Auto Scaling groups
- Use S3 as durable storage for input and intermediate data
- Implement retry logic for failed tasks
- Use Dask's adaptive scaling to adjust the number of workers based on workload
- Leverage Snowflake's scalability for data warehousing

## 7. Logging and Monitoring

- Use CloudWatch for centralized logging and monitoring of the Dask cluster
- Implement custom logging in Dask jobs for detailed processing logs
- Set up CloudWatch alarms for job failures and data quality issues
- Use Dask's dashboard for real-time monitoring of task progress and resource usage
- Utilize Snowflake's query history and resource monitoring features

## 8. Deployment and Scaling Strategy

- Use Infrastructure as Code (IaC) with AWS CloudFormation or Terraform
- Implement CI/CD pipeline for automated testing and deployment
- Use Dask-specific deployment tools for easy cluster setup
- Leverage Snowflake's ability to scale compute and storage independently

## 9. Checkpointing and State Management

- Utilize Dask's `persist()` and `compute()` methods for caching intermediate results
- Implement custom checkpointing by saving intermediate aggregation results to S3
- Use Dask's delayed API for fine-grained control over task execution and dependencies
- Leverage Snowflake's transactional capabilities for ensuring data consistency

## 10. Decoupling Compute and Data Storage

Decoupling compute and data storage allows the system to scale independently, improving efficiency and cost management:

### 10.1 Compute (Dask on EKS)
- **Scalability**: Dask can dynamically scale up or down based on the current workload, without affecting the data storage system.
- **Fault Tolerance**: Dask's fault tolerance mechanisms ensure that individual task failures do not affect the overall job, enabling reliable large-scale data processing.

### 10.2 Data Storage (S3 and Snowflake)
- **Scalability**: S3 provides virtually unlimited storage capacity, and Snowflake allows for independent scaling of compute and storage, ensuring efficient data management.
- **Durability and Availability**: S3's durability and availability guarantees ensure that data is safe and accessible, while Snowflake's architecture supports high availability and disaster recovery.

By decoupling these components, the system can efficiently handle large volumes of data and complex computations without one component becoming a bottleneck for the other.

## 11. Cost and Data Size Estimation

### 11.1 Data Size Estimation
- **Average review size**: 1 KB
- **Number of reviews per day**: 3,000,000
- **Number of reviews per year**: 1,095,000,000
- **Data size per year**: 1,095 GB ≈ 1.095 TB
- **Additional metadata and processed data**: 2x raw data size
- **Total data size per year**: 3.285 TB

### 11.2 Cost Estimation

#### 11.2.1 Compute Costs (Dask on EKS)
- **Instance type**: m5.xlarge (4 vCPUs, 16 GB RAM)
- **Cost per hour**: $0.192
- **Number of instances**: 4 (1 master, 3 worker nodes)
- **Usage per day**: 5 hours
- **Annual cost**: $0.192 * 4 * 5 * 365 = $14,016

#### 11.2.2 Storage Costs
- **S3 storage cost per GB**: $0.023
- **Total S3 storage cost per year**: 3,285 * $0.023 = $75.56 per month, $906.72 annually

#### 11.2.3 Snowflake Costs
- **Storage cost per TB per month**: $23 (compressed)
- **Total storage cost per year**: 3.285 TB * $23 * 12 = $906.66
- **Compute cost (credits)**: Assuming 2500 credits per year for compute, $2 per credit, annual cost = $5,000

### 11.3 Total Estimated Annual Cost
- **Compute (Dask on EKS)**: $14,016
- **Storage (S3)**: $906.72
- **Storage (Snowflake)**: $906.66
- **Compute (Snowflake)**: $5,000
- **Total Annual Cost**: $20,829.38
