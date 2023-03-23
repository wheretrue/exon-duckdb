"""Package for wtt01."""

from pathlib import Path
from typing import Optional, Union
import os
from string import Template

import platform
import zipfile
import tempfile

import urllib.request

import duckdb

from wtt01.__about__ import __version__, EXTENSION_NAME


__all__ = ["get_connection", "run_query_file", "__version__"]


class WTTException(Exception):
    """Base exception for wtt01."""


class WTT01ConfigurationException(WTTException):
    """Exception for wtt01 configuration."""

class WTT01QueryException(WTTException):
    """Exception for wtt01 query."""


def run_query_file(
    con: duckdb.DuckDBPyConnection,
    file_path: Union[Path, str],
    **template_vars: dict[str,str],
) -> duckdb.DuckDBPyConnection:
    """Run a query from a file."""
    if isinstance(file_path, str):
        file_path = Path(file_path)

    if template_vars:
        try:
            template = Template(file_path.read_text(encoding="utf-8"))
            query = template.substitute(**template_vars)

            return con.execute(query)
        except KeyError as exp:
            raise WTT01QueryException(
                f"Missing key {exp} in query"
            ) from exp
        except TypeError as exp:
            raise WTT01QueryException(
                f"Invalid query {exp}"
            ) from exp
        except Exception as exp:
            raise WTT01QueryException(
                f"Unknown error {exp}"
            ) from exp

    else:
        return con.execute(file_path.read_text(encoding="utf-8"))


def get_connection(
    database: str = ":memory:",
    read_only: bool = False,
    config: Optional[dict] = None,
    s3_uri: Optional[str] = None,
    file_path: Optional[Path] = None,
) -> duckdb.DuckDBPyConnection:
    """Return a connection with wtt01 loaded."""

    if "WTT_01_LICENSE" not in os.environ:
        raise WTT01ConfigurationException(
            "WTT_01_LICENSE environment variable not set. "
            "Check the docs or email at help@wheretrue.com"
        )

    if config is None:
        config = {
            "allow_unsigned_extensions": True,
        }
    else:
        config["allow_unsigned_extensions"] = True

    con = duckdb.connect(
        database=database,
        read_only=read_only,
        config=config,
    )

    try:
        con.load_extension(EXTENSION_NAME)
        return con
    except duckdb.IOException as exp:
        extension_path = os.getenv("WTT01_EXTENSION_PATH")

        if extension_path is not None:
            extension_path = Path(extension_path).absolute()
            con.install_extension(str(extension_path), force_install=True)
            con.load_extension(EXTENSION_NAME)

        elif file_path is not None and file_path.exists():
            con.install_extension(str(file_path.absolute()), force_install=True)
            con.load_extension(EXTENSION_NAME)

        elif s3_uri is not None:
            # download
            pass
        else:
            platform_uname = platform.uname()
            operating_system = platform_uname.system
            architecture = platform_uname.machine
            version = __version__

            from wtt01._env import ENVIRONMENT

            name = "wtt01"
            bucket = f"wtt-01-dist-{ENVIRONMENT}"
            filename = f"{name}-{version}-{operating_system}-{architecture}.zip"

            full_s3_path = (
                f"http://{bucket}.s3.amazonaws.com/extension/{name}/{filename}"
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_dir_path = Path(temp_dir)
                temp_file_name = temp_dir_path / filename

                try:
                    urllib.request.urlretrieve(full_s3_path, temp_file_name)
                except Exception as retrieve_exp:
                    raise WTTException(
                        f"Unable to download extension from {full_s3_path}"
                    ) from retrieve_exp

                with zipfile.ZipFile(temp_file_name, "r") as zip_ref:
                    zip_ref.extract(f"{name}.duckdb_extension", temp_dir)

                output_file = temp_dir_path / f"{name}.duckdb_extension"
                if not output_file.exists():
                    raise WTTException(
                        f"Unable to find extension file at {output_file}"
                    ) from exp

                con.install_extension(output_file.as_posix(), force_install=True)

                con.load_extension(EXTENSION_NAME)

    return con
