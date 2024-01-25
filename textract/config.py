import os
from pathlib import Path
from dotenv import load_dotenv

dotenv_path = Path("../.env")
load_dotenv(dotenv_path=dotenv_path)


################ AWS ################
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")


################ S3 ################
SOURCE_BUCKET_NAME = "test-bucket-653659"
UPLOAD_BUCKER_NAME = "test-images-textract-outputs"
