import boto3
from pathlib import Path
from dotenv import load_dotenv
from config import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION

dotenv_path = Path("../.env")
load_dotenv(dotenv_path=dotenv_path)


def get_boto_session():
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )

    return session
