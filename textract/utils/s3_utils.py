import os


def upload_folder(s3_client, bucket, local_directory, destination):
    if not os.path.exists(local_directory):
        print(f"{local_directory} doesn't exist. Please check the filepath once.")
        raise FileNotFoundError

    for root, dirs, files in os.walk(local_directory):
        for filename in files:
            local_path = os.path.join(root, filename)

            relative_path = os.path.relpath(local_path, local_directory)
            s3_path = os.path.join(destination, relative_path)
            # Uncomment below if working in Windows
            # s3_path = s3_path.replace("\\", "/")

            print(f"Uploading {s3_path}")
            s3_client.upload_file(local_path, bucket, s3_path)


def list_bucket_contents(s3_connection, bucket_name, name_prefix=""):
    source_bucket_conn = s3_connection.Bucket(bucket_name)

    pdf_filenames = []
    for object_summary in source_bucket_conn.objects.filter(Prefix=name_prefix):
        fname = object_summary.key
        if fname.endswith(".pdf"):
            pdf_filenames.append(fname)

    return pdf_filenames
