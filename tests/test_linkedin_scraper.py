import importlib

import pytest


def test_linkedin_stub():
    with pytest.raises(NotImplementedError):
        importlib.import_module("ingestion.scrapers.linkedin")
