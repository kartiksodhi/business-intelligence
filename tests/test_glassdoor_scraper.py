import importlib

import pytest


def test_glassdoor_stub():
    with pytest.raises(NotImplementedError):
        importlib.import_module("ingestion.scrapers.glassdoor")

