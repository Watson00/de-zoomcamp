from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days =1))
def fetch(dataset_file:str)-> pd.DataFrame:
    """Read taxi data from web into pandas Dataframe"""
    # if randint(0,1) > 0:
    #     raise Exception

    df = pd.read_csv(f"/mnt/c/Users/watso/Desktop/DE_Zoomcamp/Week3/data/{dataset_file}.csv.gz")
    return df

@task()
def write_local(df: pd.DataFrame, dataset_file:str) -> Path:
    """Write Dataframe to parquet file"""
    path = Path(f"/mnt/c/Users/watso/Desktop/DE_Zoomcamp/Week3/data/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    # df["pickup_datetime"] = pd.to_datetime(df['pickup_datetime'])
    # df["dropoff_datetime"] = pd.to_datetime(df['dropoff_datetime'])
    # print(df.head(2))
    # print(f"columns: {df.dtypes}")
    # print(f"rows: {len(df)}")
    return df

# @task()
# def write_gcs(path: Path, dataset_file):
#     """Upload to GCS"""
#     gcs_block = GcsBucket.load("gcs-zoom")
#     gcs_block.upload_from_path(from_path=f"{path}",to_path=f"{dataset_file}.parquet")
#     return

@flow()
def etl_web_to_gcs(year: int, month: int,  ) -> None:
    """The main ETL function"""
 
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{dataset_file}.csv.gz"

    df = fetch(dataset_file)
    df_clean = clean(df)
    path = write_local(df_clean, dataset_file)
    #write_gcs(path, dataset_file)

@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021):

    for month in months:
        etl_web_to_gcs(year,month)


if __name__ == '__main__': 
    
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    year = 2019
    etl_parent_flow(months, year)