import os

from utils.processing import table_extraction, image_extraction
from utils.textract_utils import start_job, is_job_complete, get_job_results
from utils.s3_utils import upload_folder
from utils.logger import CustomLogger


def e2e_textract_pipeline(
    session, pdf_filename, logger, source_bucket, destination_bucket
):
    if logger is None:
        project_name = "textract"
        logger = CustomLogger(log_file=f"{project_name}.logs", name=project_name)

    logger.info(f"Running pipeline for {pdf_filename}")
    textract_client = session.client("textract")
    s3_client = session.client("s3")

    job_id = start_job(textract_client, source_bucket, pdf_filename)
    
    logger.info("Started job with id: {}".format(job_id))
    if is_job_complete(textract_client, job_id):
        responses = get_job_results(textract_client, job_id)

    logger.debug("Running table extraction")
    table_extraction(responses=responses, pdf_filename=pdf_filename)

    logger.debug("Running image extraction")
    image_extraction(responses=responses, pdf_filename=pdf_filename)

    logger.debug("Uploading extracted info")

    pdf_folder = pdf_filename.replace(".pdf", "")
    local_directory = os.path.join("outputs", pdf_folder)
    upload_folder(
        s3_client, destination_bucket, local_directory, destination=local_directory
    )

    logger.debug("Uploading metadata")
    local_directory = os.path.join("outputs", "metadata", pdf_folder)
    upload_folder(
        s3_client, destination_bucket, local_directory, destination=local_directory
    )

    logger.info("Pipeline run complete!")


if __name__ == "__main__":
    from config import SOURCE_BUCKET_NAME, UPLOAD_BUCKER_NAME
    from utils.generic_utils import get_boto_session
    from utils.s3_utils import list_bucket_contents

    project_name = "textract"
    logger = CustomLogger(log_file=f"{project_name}.logs", name=project_name)

    session = get_boto_session()
    s3_connection = session.resource("s3")
    source_bucket_conn = s3_connection.Bucket(SOURCE_BUCKET_NAME)

    pdf_filenames = list_bucket_contents(s3_connection, SOURCE_BUCKET_NAME)

    # print(pdf_filenames[8:])
    for pdf_filename in pdf_filenames[8:9]:
        e2e_textract_pipeline(
            session=session,
            pdf_filename=pdf_filename,
            logger=logger,
            source_bucket=SOURCE_BUCKET_NAME,
            destination_bucket=UPLOAD_BUCKER_NAME,
        )
