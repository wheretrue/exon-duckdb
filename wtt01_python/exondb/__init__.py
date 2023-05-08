"""Package for exondb."""

from exondb.__about__ import __version__
from exondb.main import get_connection, run_query_file

__all__ = ["get_connection", "run_query_file", "__version__"]
