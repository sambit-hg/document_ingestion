import os
import json
import pandas as pd
from io import StringIO
from trp import Document


#######################################
# Parsing tables from Textract output #
#######################################
def table_extraction(responses, pdf_filename):
    """
    Saves extracted tables and their corresponding metadata from Textract response

    - HTML tables located inside {pdf_filename}/tables folder
    - metadata is  located inside metadata/{pdf_filename}/tables folder

    """
    output_folder = "outputs"
    os.makedirs(output_folder, exist_ok=True)

    doc = Document(responses)

    pdf_name_dir = pdf_filename.replace(".pdf", "")

    TABLE_SAVE_PATH = os.path.join(output_folder, pdf_name_dir, "tables")
    METADATA_SAVE_PATH = os.path.join(output_folder, "metadata", pdf_name_dir, "tables")

    os.makedirs(TABLE_SAVE_PATH, exist_ok=True)
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

    - Image text located inside output/{pdf_filename}/image folder
    - metadata is  located inside output/metadata/{pdf_filename}/table folder

    """
    output_folder = "outputs"
    os.makedirs(output_folder, exist_ok=True)

    hashed_blocks, detected_images = get_image_blocks_from_response(responses)

    for i, image_block in enumerate(detected_images):
        # Getting all the text blocks belonging to detected image block
        child_block_ids = []
        relationships = image_block.get("Relationships", [])
        if len(relationships) > 0:
            for relation in relationships:
                if relation.get("Type") == "CHILD":
                    child_block_ids.extend(relation.get("Ids", []))

        pdf_name_dir = pdf_filename.replace(".pdf", "")

        IMAGE_TEXT_SAVE_PATH = os.path.join(output_folder, pdf_name_dir, "image")
        METADATA_SAVE_PATH = os.path.join(
            output_folder, "metadata", pdf_name_dir, "image"
        )

        os.makedirs(IMAGE_TEXT_SAVE_PATH, exist_ok=True)
        os.makedirs(METADATA_SAVE_PATH, exist_ok=True)

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

        metadata_json_save_path = os.path.join(
            METADATA_SAVE_PATH, save_text_filename + ".metadata.json"
        )

        # Only saving for images with detected texts
        if len(child_block_ids) > 0:
            save_all_child_texts(hashed_blocks, child_block_ids, save_text_filepath)
            json.dump(metadata, open(metadata_json_save_path, "w"), indent=4)
