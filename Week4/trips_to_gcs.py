from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import os

@task(retries=3)
def fetch(dataset_url:str)-> pd.DataFrame:
    """Read taxi data from web into pandas Dataframe"""
    # if randint(0,1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file:str) -> Path:
    """Write Dataframe to parquet file"""
    #path = Path(f"/mnt/c/Users/watso/Desktop/DE_Zoomcamp/Week2/data/{color}/{dataset_file}.parquet")
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df['tpep_pickup_datetime'])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df["passenger_count"] =df['passenger_count'].astype('Int64')
    df["RatecodeID"] =df['RatecodeID'].astype('Int64')
    df["payment_type"] =df['payment_type'].astype('Int64')
    #df["trip_type"] =df['trip_type'].astype('Int64')
    df["VendorID"] =df['VendorID'].astype('Int64')

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_gcs(path: Path, color,dataset_file):
    """Upload to GCS"""
    gcs_block = GcsBucket.load("trip-bucket")
    gcs_block.upload_from_path(from_path=f"{path}",to_path=f"{color}/{dataset_file}.parquet",timeout=9000)
    return

@flow()
def etl_web_to_gcs(year: int, month: int,  color: str) -> None:
    """The main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    # file_path = Path('f"data/{color}/{dataset_file}.parquet')
    # if os.path.exists(file_path):
    #     print('File already exists in the path. Skipping...')
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path, color,dataset_file)

@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2019, color: str = "yellow"):

    for month in months:
        etl_web_to_gcs(year,month, color)
        
if __name__ == '__main__': 
    color = "yellow"
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    #1,2,3,4,5,6,7,8,9,10,11,12
    year = 2020
    etl_parent_flow(months, year, color)
    print("Load completed succesfully")