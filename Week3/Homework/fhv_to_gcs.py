from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
import requests


@task()
def download(dataset_url: str, dataset_file: str):

    path = Path(f"/mnt/c/Users/watso/Desktop/DE_Zoomcamp/Week3/data/{dataset_file}.csv.gz")
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    r = requests.get(dataset_url, allow_redirects=True)

    open(path, 'wb').write(r.content)
  

@task()
def write_gcs(dataset_file):
    """Upload to GCS"""
    path = f"/mnt/c/Users/watso/Desktop/DE_Zoomcamp/Week3/data/{dataset_file}.csv.gz"
    gcs_block = GcsBucket.load("gcs-zoom")
    gcs_block.upload_from_path(from_path=f"{path}",to_path=f"data/{dataset_file}.csv.gz")
    return

@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
 
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    downloads = download(dataset_url,dataset_file)
    write_gcs(dataset_file)

@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021,):

    for month in months:
        etl_web_to_gcs(year,month)


if __name__ == '__main__': 
    
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    year = 2019
    etl_parent_flow(months, year)