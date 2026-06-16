from __future__ import annotations

import pytest

import shared.metrics as metrics


def test_safe_get_route_name_falls_back_for_included_router_path_error(monkeypatch):
    def _raise_included_router_error(_request):
        raise AttributeError("'_IncludedRouter' object has no attribute 'path'")

    monkeypatch.setattr(metrics, "_ORIGINAL_GET_ROUTE_NAME", _raise_included_router_error)

    assert metrics._safe_get_route_name(object()) == "__unknown__"


def test_safe_get_route_name_reraises_unrelated_attribute_error(monkeypatch):
    def _raise_other_attribute_error(_request):
        raise AttributeError("something else")

    monkeypatch.setattr(metrics, "_ORIGINAL_GET_ROUTE_NAME", _raise_other_attribute_error)

    with pytest.raises(AttributeError, match="something else"):
        metrics._safe_get_route_name(object())
