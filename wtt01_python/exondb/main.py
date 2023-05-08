"""Package for exondb."""

from pathlib import Path
from typing import Optional, Union
import os
from string import Template
import shutil
import gzip

import platform
import tempfile

import urllib.request

import duckdb

from exondb.__about__ import __version__, EXTENSION_NAME


__all__ = ["get_connection", "run_query_file", "__version__"]


class WTTException(Exception):
    """Base exception for exondb."""


class WTT01ConfigurationException(WTTException):
    """Exception for exondb configuration."""


class WTT01QueryException(WTTException):
    """Exception for exondb query."""


def run_query_file(
    con: duckdb.DuckDBPyConnection, file_path: Union[Path, str], **template_vars: dict
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
            raise WTT01QueryException(f"Missing key {exp} in query") from exp
        except TypeError as exp:
            raise WTT01QueryException(f"Invalid query {exp}") from exp
        except Exception as exp:
            raise WTT01QueryException(f"Unknown error {exp}") from exp

    else:
        return con.execute(file_path.read_text(encoding="utf-8"))


def get_connection(
    database: str = ":memory:",
    read_only: bool = False,
    config: Optional[dict] = None,
    file_path: Optional[Path] = None,
) -> duckdb.DuckDBPyConnection:
    """Return a connection with exondb loaded."""

    if "EXONDB_LICENSE" not in os.environ:
        raise WTT01ConfigurationException(
            "EXONDB_LICENSE environment variable not set. "
            "Check the docs or email at help@wheretrue.com"
        )

    if config is None:
        config = {"allow_unsigned_extensions": True}
    else:
        config["allow_unsigned_extensions"] = True

    con = duckdb.connect(database=database, read_only=read_only, config=config)

    try:
        con.load_extension(EXTENSION_NAME)
        return con
    except duckdb.IOException as exp:
        extension_path = os.getenv("EXONDB_EXTENSION_PATH")

        if extension_path is not None:
            extension_path = Path(extension_path).absolute()
            con.install_extension(str(extension_path), force_install=True)
            con.load_extension(EXTENSION_NAME)

        elif file_path is not None and file_path.exists():
            con.install_extension(str(file_path.absolute()), force_install=True)
            con.load_extension(EXTENSION_NAME)

        else:
            version = __version__
            name = "exondb"

            platform_uname = platform.uname()
            operating_system = platform_uname.system
            architecture = platform_uname.machine

            if operating_system.lower() == "windows":
                duckdb_arch = "windows_amd64"
            elif operating_system.lower() == "darwin" and architecture.lower() == "x86_64":
                duckdb_arch = "osx_amd64"
            elif operating_system.lower() == "darwin" and architecture.lower() == "arm64":
                duckdb_arch = "osx_arm64"
            elif operating_system.lower() == "linux" and architecture.lower() == "x86_64":
                duckdb_arch = "linux_amd64_gcc4"
            else:
                raise WTTException(
                    f"Unable to find extension for {operating_system} {architecture}"
                )

            filename = f"{name}.duckdb_extension.gz"
            url = f"https://dbe.wheretrue.com/{name}/{version}/v0.7.1/{duckdb_arch}/{filename}"

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_dir_path = Path(temp_dir)

                temp_file_name = temp_dir_path / filename

                try:
                    urllib.request.urlretrieve(url, temp_file_name)
                except Exception as retrieve_exp:
                    raise WTTException(
                        f"Unable to download extension from {url}"
                    ) from retrieve_exp

                # ungzip the file
                output_file = temp_dir_path / filename.rstrip(".gz")
                with gzip.open(temp_file_name, "rb") as f_in:
                    with open(output_file, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                if not output_file.exists():
                    raise WTTException(
                        f"Unable to find extension file at {output_file}"
                    ) from exp

                con.install_extension(output_file.as_posix(), force_install=True)
                con.load_extension(EXTENSION_NAME)

    return con
