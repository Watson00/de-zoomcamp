from prefect.deployments import Deployment
from etl_web_to_gcs import etl_parent_flow
from prefect.filesystems import GitHub 


github_block = GitHub.load("github-repo")

deployment = Deployment.build_from_flow(
     flow=etl_parent_flow,
     name="hw-question4",
     storage=github_block,
     entrypoint="week2/homework/etl_web_to_gcs.py:etl_parent_flow")

if __name__ == "__main__":
    deployment.apply()