## 6. Ingestion and Aggregation

**This task was completed with modifications to the original requirements of Exercise 6** [https://github.com/danielbeach/data-engineering-practice](https://github.com/danielbeach/data-engineering-practice)

Changes:
- Apache Spark was replaced by another MPP engine - Trino
- Trino SQL instead of using Python
- ELT approach instead of ETL

#### Setup

```bash

# change directory to Task-06 
cd data-engineering-tasks/Task-06

# download minio client (local s3 storage)
curl https://dl.min.io/client/mc/release/linux-amd64/mc -o $HOME/.local/bin/mc
chmod +x $HOME/.local/bin/mc

# clone Trino engine
git clone git@github.com:x2nur/trino-hive-s3.git trino
# init engine
trino/trino-init.sh
# start engine
trino/trino-start.sh

# set up local s3 service 
# mc alias set ALIAS_NAME S3_SVC_ADDRESS USER PASSWORD
mc alias set trino localhost trino trino-Password

```

#### Problems Statement

The task is to load csv files from data directory (Divvy_Trips_2019_Q4.csv.gz) with `Trino` and answer the following questions.

1. What is the `average` trip duration per day?
2. How many trips were taken each day?
3. What was the most popular starting trip station for each month?
4. What were the top 3 trip stations each day for the last two weeks?
5. Do `Male`s or `Female`s take longer trips on average?
6. What is the top 10 ages of those that take the longest trips, and shortest?


#### Preparing and loading the data

```bash
# a bucket for raw data files
mc mb trino/raw
# a bucket for hive schemas
mc mb trino/dwh
# copy the data archive to the bucket for raw data files
mc cp data/Divvy_Trips_2019_Q4.csv.gz trino/raw/trips/
# load
trino/exec-sql.sh < load.sql

```

#### Questions and answers

```bash

# 1. What is the average trip duration per day?
trino/exec-sql.sh < reports/report-1.sql

# 2. How many trips were taken each day?
trino/exec-sql.sh < reports/report-2.sql

# 3. What was the most popular starting trip station for each month?
trino/exec-sql.sh < reports/report-3.sql

# 4. What were the top 3 trip stations each day for the last two weeks?
trino/exec-sql.sh < reports/report-4.sql

# 5. Do Males or Females take longer trips on average?
trino/exec-sql.sh < reports/report-5.sql

# 6. What is the top 10 ages of those that take the longest trips, and shortest?
trino/exec-sql.sh < reports/report-6.sql

```

#### Cleanup

```bash
# drop tables and schema
trino/exec-sql.sh < cleanup.sql

# remove files and buckets from local s3
mc rm trino/raw/trips/Divvy_Trips_2019_Q4.csv.gz
mc rb trino/raw trino/dwh

# stop engine
trino/trino-stop.sh
# destroy engine
trino/trino-destroy.sh
```