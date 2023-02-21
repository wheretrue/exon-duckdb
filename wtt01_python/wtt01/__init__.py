"""Package for wtt01."""

from wtt01.__about__ import __version__
from wtt01.main import get_connection, run_query_file

__all__ = ["get_connection", "run_query_file", "__version__"]
