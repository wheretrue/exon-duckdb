"""Helper script to upload artifacts"""
import gzip
import os
import shutil
import platform
from pathlib import Path
import argparse

import boto3

client = boto3.client("s3")
lambda_client = boto3.client("lambda")

if __name__ == "__main__":
    handler = "EXONArtifactHandler"

    # Setup argparse to parse the arguments
    parser = argparse.ArgumentParser(
        description="Upload artifacts to S3 and invoke the artifact handler."
    )

    parser.add_argument("--name", default="exondb")

    parser.add_argument("--version", default="v0.3.9")

    parser.add_argument("--duckdb_version", default="v0.8.0")

    # add a flag to signify gcc4 or not
    parser.add_argument("--gcc4", action="store_true")

    args = parser.parse_args()

    name = args.name
    version = args.version
    duckdb_version = args.duckdb_version
    gcc4 = args.gcc4

    environment = os.environ["ENVIRONMENT"]
    bucket = f"wtt-01-dist-{environment}"

    print(f"Uploading artifacts to {bucket} with handler {handler}")
    print(f"Working on environment: {environment}")

    platform_uname = platform.uname()
    operating_system = platform_uname.system
    architecture = platform_uname.machine

    print(f"Operating system: {operating_system}")
    print(f"Architecture: {architecture}")

    if operating_system.lower() == "windows":
        arch = "windows_amd64"
    elif operating_system.lower() == "darwin" and architecture.lower() == "x86_64":
        arch = "osx_amd64"
    elif operating_system.lower() == "linux" and architecture.lower() == "x86_64":
        arch = "linux_amd64"

        if gcc4:
            arch = "linux_amd64_gcc4"

    elif operating_system.lower() == "darwin" and architecture.lower() == "arm64":
        arch = "osx_arm64"
    else:
        raise Exception(f"Unsupported platform: {operating_system} {architecture}")

    filename = f"{name}-{version}-{operating_system}-{architecture}.zip"
    full_s3_path = f"s3://{bucket}/extension/{name}/{filename}"

    local_file = Path(f"{name}.duckdb_extension.zip")

    if operating_system.lower() == "windows":
        build_target = (
            Path("build")
            / "release"
            / "extension"
            / name
            / f"{name}.duckdb_extension"
        )
    else:
        build_target = (
            Path("build") / "release" / "extension" / name / f"{name}.duckdb_extension"
        )

    # gzip the build_target with python
    gzip_build_target = f"{name}.duckdb_extension.gz"
    with open(build_target, "rb") as f_in:
        with gzip.open(gzip_build_target, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    # aws s3 cp $1.duckdb_extension.gz s3://$5/$1/$2/$3/$4/$1.duckdb_extension.gz --acl public-read
    duckdb_path = f"{name}/{version}/{duckdb_version}/{arch}/{name}.duckdb_extension.gz"
    client.upload_file(gzip_build_target, bucket, duckdb_path)
    client.put_object_acl(ACL="public-read", Bucket=bucket, Key=duckdb_path)

    latest_duckdb_path = (
        f"{name}/latest/{duckdb_version}/{arch}/{name}.duckdb_extension.gz"
    )

    client.copy_object(
        ACL="public-read",
        Bucket=bucket,
        CopySource=f"{bucket}/{duckdb_path}",
        Key=latest_duckdb_path,
    )
