--Setup
CREATE OR REPLACE EXTERNAL TABLE `de-ny-taxis.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://fhv-bucket2/fhv_tripdata_2019-*.csv.gz']
);

CREATE OR REPLACE TABLE de-ny-taxis.trips_data_all.fhv_tripdata AS
SELECT * FROM de-ny-taxis.trips_data_all.external_fhv_tripdata;

--Question 1
SELECT COUNT(*) FROM de-ny-taxis.trips_data_all.fhv_tripdata; --43,244,696

--Question 2
SELECT DISTINCT COUNT(affiliated_base_number) FROM de-ny-taxis.trips_data_all.external_fhv_tripdata; --0 MB


SELECT DISTINCT COUNT(affiliated_base_number) FROM de-ny-taxis.trips_data_all.fhv_tripdata; -- 317.94 MB

--Question 3
SELECT count(*) FROM de-ny-taxis.trips_data_all.fhv_tripdata
WHERE IFNULL(PUlocationID, 909) = 909 and IFNULL(DOlocationID, 908) = 908; --717,748

--Question 4 
CREATE OR REPLACE TABLE de-ny-taxis.trips_data_all.fhv_tripdata_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM de-ny-taxis.trips_data_all.external_fhv_tripdata;

--Question 5
SELECT DISTINCT affiliated_base_number 
FROM de-ny-taxis.trips_data_all.fhv_tripdata_partitoned_clustered
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31' --23.06MB

SELECT DISTINCT affiliated_base_number 
FROM de-ny-taxis.trips_data_all.fhv_tripdata
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31' --647.97MB

--Question 6: GCP Bucket

--Question 7: True

--Question 8
CREATE OR REPLACE EXTERNAL TABLE `de-ny-taxis.trips_data_all.external_fhv_tripdata_parquet`
OPTIONS (
  format = 'CSV',
  uris = ['gs://fhv-bucket2/fhv_tripdata_2019-*.csv.gz']
);

CREATE OR REPLACE TABLE de-ny-taxis.trips_data_all.fhv_tripdata_parquet AS
SELECT * FROM de-ny-taxis.trips_data_all.external_fhv_tripdatat_parquet;

SELECT COUNT(*) FROM de-ny-taxis.trips_data_all.external_fhv_tripdata_parquet;
