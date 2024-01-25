import os
import time
import json
import boto3
import pandas as pd
from io import StringIO
from trp import Document
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


#####################################
# Textract utils for job submission #
#####################################
def start_job(client, s3_bucket_name, object_name):
    response = None
    response = client.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": s3_bucket_name, "Name": object_name}},
        FeatureTypes=["TABLES", "LAYOUT"],
    )

    return response["JobId"]


def is_job_complete(client, job_id):
    time.sleep(1)
    response = client.get_document_analysis(JobId=job_id)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while status == "IN_PROGRESS":
        time.sleep(1)
        response = client.get_document_analysis(JobId=job_id)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status


def get_job_results(client, job_id):
    pages = []

    response = client.get_document_analysis(JobId=job_id)
    pages.append(response)
    print("Resultset page received: {}".format(len(pages)))

    next_token = None
    if "NextToken" in response:
        next_token = response["NextToken"]

    while next_token:
        response = client.get_document_analysis(JobId=job_id, NextToken=next_token)
        pages.append(response)
        print("Resultset page received: {}".format(len(pages)))
        next_token = None
        if "NextToken" in response:
            next_token = response["NextToken"]

    return pages


#######################################
# Parsing tables from Textract output #
#######################################
def table_extraction(responses, pdf_filename):
    """
    Saves extracted tables and their corresponding metadata from Textract response

    - HTML tables located inside {pdf_filename}/tables folder
    - metadata is  located inside metadata/{pdf_filename}/tables folder

    """
    doc = Document(responses)

    pdf_name_dir = pdf_filename.replace(".pdf", "")

    TABLE_SAVE_PATH = os.path.join(pdf_name_dir, "tables")
    os.makedirs(TABLE_SAVE_PATH, exist_ok=True)

    METADATA_SAVE_PATH = os.path.join("metadata", TABLE_SAVE_PATH)
    os.makedirs(METADATA_SAVE_PATH, exist_ok=True)

    for page_number, page in enumerate(doc.pages):
        page_number += 1

        for table_num, table in enumerate(page.tables):
            # Creating table CSV string
            csv = ""
            for r, row in enumerate(table.rows):
                for c, cell in enumerate(row.cells):
                    csv += "{}".format(cell.text) + "\t"
                csv += "\n"

            df = pd.read_csv(StringIO(csv), sep="\t", header=None)
            df = df.dropna(axis=1, how="all")

            save_filename = (
                f"{pdf_filename}__page__{page_number}__tableidx__{table_num}.html"
            )
            save_path = os.path.join(TABLE_SAVE_PATH, save_filename)

            # CSV string is converted to HTML and <html> and <body> tags
            # are inserted to make it parsable by Kendra
            df_html_string = (
                "<html><body>"
                + df.to_html(index=False, header=False)
                + "</body></html>"
            )

            with open(save_path, "w") as f:
                f.write(df_html_string)

            # Metadata json creation with attributes for page number
            # and source PDF the table belongs to
            metadata = {}
            metadata["DocumentId"] = save_filename
            metadata["Attributes"] = {
                "page_number": page_number,
                "source_pdf": pdf_filename,
            }

            metadata_json_save_path = os.path.join(
                METADATA_SAVE_PATH, save_filename + ".metadata.json"
            )

            json.dump(metadata, open(metadata_json_save_path, "w"), indent=4)


#######################################
# Parsing images from Textract output #
#######################################
def get_image_blocks_from_response(responses):
    """
    Filter image blocks (tagged as LAYOUT_FIGURE) from Textract response
    """
    hashed_blocks = {}

    for response_obj in responses:
        for block in response_obj["Blocks"]:
            hashed_blocks[block["Id"]] = block

    detected_images = []
    for response_obj in responses:
        for block in response_obj["Blocks"]:
            if block["BlockType"] == "LAYOUT_FIGURE":
                detected_images.append(block)

    return hashed_blocks, detected_images


def save_all_child_texts(hashed_blocks, child_block_ids, save_text_filepath):
    text = " ".join(
        [hashed_blocks[block_id].get("Text", "") for block_id in child_block_ids]
    )

    with open(save_text_filepath, "w") as f:
        f.write(text)


def image_extraction(responses, pdf_filename):
    """
    Saves text in extracted image and their corresponding metadata from Textract response

    - Image text located inside {pdf_filename}/image folder
    - metadata is  located inside metadata/{pdf_filename}/table folder

    """
    hashed_blocks, detected_images = get_image_blocks_from_response(responses)

    for i, image_block in enumerate(detected_images):
        # Getting all the text blocks belonging to detected image block
        child_block_ids = []
        relationships = image_block.get("Relationships", [])
        if len(relationships) > 0:
            for relation in relationships:
                if relation.get("Type") == "CHILD":
                    child_block_ids.extend(relation.get("Ids", []))

        IMAGE_TEXT_SAVE_PATH = os.path.join(pdf_filename.replace(".pdf", ""), "image")
        os.makedirs(IMAGE_TEXT_SAVE_PATH, exist_ok=True)

        page_number = image_block["Page"] + 1
        save_text_filename = f"{pdf_filename}__page__{page_number}__imgidx__{i}.txt"
        save_text_filepath = os.path.join(IMAGE_TEXT_SAVE_PATH, save_text_filename)

        # Metadata json creation with attributes for
        # page number and source PDF the table belongs to
        metadata = {}
        metadata["DocumentId"] = save_text_filename
        metadata["Attributes"] = {
            "page_number": page_number,
            "source_pdf": pdf_filename,
        }

        METADATA_SAVE_PATH = os.path.join("metadata", IMAGE_TEXT_SAVE_PATH)
        os.makedirs(METADATA_SAVE_PATH, exist_ok=True)

        metadata_json_save_path = os.path.join(
            METADATA_SAVE_PATH, save_text_filename + ".metadata.json"
        )

        # Only saving for images with detected texts
        if len(child_block_ids) > 0:
            save_all_child_texts(hashed_blocks, child_block_ids, save_text_filepath)
            json.dump(metadata, open(metadata_json_save_path, "w"), indent=4)


###########################
# Uploading results to S3 #
###########################
def upload_folder(s3_client, bucket, local_directory, destination):
    for root, dirs, files in os.walk(local_directory):
        for filename in files:
            local_path = os.path.join(root, filename)

            relative_path = os.path.relpath(local_path, local_directory)
            s3_path = os.path.join(destination, relative_path)
            # Uncomment below if working in Windows
            # s3_path = s3_path.replace("\\", "/")

            print(f"Uploading {s3_path}")
            s3_client.upload_file(local_path, bucket, s3_path)
