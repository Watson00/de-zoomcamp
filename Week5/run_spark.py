python3 Spark_SQL.py \
    --input_green=data/pq/green/2020/*/ \
    --input_yellow=data/pq/yellow/2020/*/ \
    --output=data/report-2020

URL="spark://Watson-Desk.:7077"

spark-submit \
    --master="${URL}" \
    Spark_SQL.py \
        --input_green=data/pq/green/2021/*/ \
        --input_yellow=data/pq/yellow/2021/*/ \
        --output=data/report-2021

        --input_green=gs://spark-taxi/pq/green/2021/*/ \
        --input_yellow=gs://spark-taxi/pq/yellow/2021/*/ \
        --output=gs://spark-taxi/report-2021

{

gcloud dataproc jobs submit pyspark \
    --cluster=de-taxis \
    --region=us-central1 \
    gs://spark-taxi/code/spark_sql_bigquery.py \
    -- \
        --input_green=gs://spark-taxi/pq/green/2020/*/ \
        --input_yellow=gs://spark-taxi/pq/yellow/2020/*/ \
        --output=gs://spark-taxi/report-2020

gcloud dataproc jobs submit pyspark \
    --cluster=de-taxis \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://spark-taxi/code/spark_sql_bigquery.py \
    -- \
        --input_green=gs://spark-taxi/pq/green/2020/*/ \
        --input_yellow=gs://spark-taxi/pq/yellow/2020/*/ \
        --output=trips_data_all.reports-2020
