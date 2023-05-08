"""Test the extension can be loaded and used."""

import pathlib

import pytest

import exondb.main as main

HERE = pathlib.Path(__file__).parent


def test_load(monkeypatch):
    """Test the extension can be loaded."""

    monkeypatch.setenv("EXONDB_LICENSE", "test")

    con = main.get_connection()

    results = con.execute("SELECT gc_content('ATGC');").fetchall()
    assert len(results) == 1


def test_load_missing_env_var():
    """Test the extension can be loaded."""

    with pytest.raises(main.WTT01ConfigurationException):
        main.get_connection()
