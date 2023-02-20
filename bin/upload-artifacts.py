"""Helper script to upload artifacts"""
import os
import json
import uuid
import zipfile
import platform
from pathlib import Path

import boto3

client = boto3.client("s3")
lambda_client = boto3.client("lambda")

if __name__ == "__main__":
    environment = os.environ["ENVIRONMENT"]
    bucket = os.environ["ARTIFACT_BUCKET"]
    handler = os.environ["ARTIFACT_HANDLER"]

    print(f"Uploading artifacts to {bucket} with handler {handler}.")
    print(f"Working on environment: {environment}.")

    name = "wtt01"

    platform_uname = platform.uname()
    operating_system = platform_uname.system
    architecture = platform_uname.machine

    version = "0.1.8"

    filename = f"{name}-{version}-{operating_system}-{architecture}.zip"
    full_s3_path = f"s3://{bucket}/extension/{name}/{filename}"

    local_file = Path(f"{name}.duckdb_extension.zip")

    if operating_system.lower() == "windows":
        raise RuntimeError("Windows is not supported yet.")
        # if cmake_build_type == "Release":
        #     build_target = (
        #         Path("build") / "extension" / name / f"{name}.duckdb_extension"
        #     )
        # else:
        #     build_target = (
        #         Path("build")
        #         / "release"
        #         / "extension"
        #         / "wtt01"
        #         / f"{name}.duckdb_extension"
        #     )
    else:
        build_target = Path("build") / "release" / "extension" / name / f"{name}.duckdb_extension"

    with zipfile.ZipFile(local_file, "w") as zip_file:
        print(f"Adding {build_target} to {local_file}.")
        zip_file.write(str(build_target), arcname=f"{name}.duckdb_extension")

    # Copy local_file to s3 location molcaat_full_s3_path.

    upload_key = f"extension/{name}/{filename}"
    client.upload_file(str(local_file), bucket, upload_key)

    # Change the ACL of the file to public-read.
    client.put_object_acl(
        ACL="public-read",
        Bucket=bucket,
        Key=upload_key,
    )

    body = json.dumps(
        {
            "http": {"method": "POST"},
            "body": {
                "id": uuid.uuid4().hex,
                "s3_key": full_s3_path,
                "os": operating_system,
                "arch": architecture,
                "tag": version,
                "tool": name,
                "environment": environment,
            },
        }
    )
    print(body)

    # Invoke the lambda function ARTIFACT_HANDLER with the body.
    response = lambda_client.invoke(
        FunctionName=handler,
        Payload=bytes(body, "utf-8"),
    )
    print(response)
