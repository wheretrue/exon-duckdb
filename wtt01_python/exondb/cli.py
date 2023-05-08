"""Add a CLI to the Python package."""
import argparse

from exondb.__about__ import __version__
from exondb import run_query_file, get_connection

def query(args):
    """Run a query."""
    con = get_connection(database=args.database)

    template_vars = {}
    for template_arg in args.template_args:
        key, value = template_arg.split("=")
        template_vars[key] = value

    run_query_file(con, args.query, **template_vars)

def main():
    """Console script for exondb."""

    # Setup the parser
    parser = argparse.ArgumentParser(
        prog="exondb",
        description="exondb Python CLI",
    )

    subparsers = parser.add_subparsers(
        title="subcommands",
        description="valid subcommands",
        help="additional help",
    )

    # Setup the parser for the "version" subcommand
    parser_version = subparsers.add_parser(
        "version",
        help="print the version",
    )
    parser_version.set_defaults(func=lambda x: print(__version__))

    # Setup the parser for the "query" subcommand
    parser_query = subparsers.add_parser(
        "query",
        help="run a query",
    )
    parser_query.add_argument(
        "query",
        help="the query to run",
    )
    parser_query.add_argument(
        "-d",
        "--database",
        help="the database to connect to",
        default=":memory:",
    )
    parser_query.add_argument(
        "-t",
        "--template-args",
        help="the template arguments",
        metavar="key=value",
        nargs="+",
        default=[],
    )

    parser_query.set_defaults(func=query)

    # Parse the arguments
    args = parser.parse_args()
    args.func(args)
