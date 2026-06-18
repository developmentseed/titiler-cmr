"""Tests for titiler.cmr.reader."""

from titiler.cmr.reader import _parse_dsl


def test_parse_dsl_parses_selector_methods() -> None:
    """Parse selector strings into dimension, values, and optional method."""
    assert _parse_dsl(["time=nearest::2024-01-01", "band=1", "band=2"]) == [
        {
            "dimension": "time",
            "values": ["2024-01-01"],
            "method": "nearest",
        },
        {
            "dimension": "band",
            "values": ["1", "2"],
            "method": None,
        },
    ]


def test_parse_dsl_rejects_multiple_methods_for_dimension() -> None:
    """Reject conflicting selection methods for the same dimension."""
    try:
        _parse_dsl(["time=nearest::2024-01-01", "time=pad::2024-01-02"])
    except ValueError as exc:
        assert "Multiple selection methods provided for dimension time" in str(exc)
    else:
        raise AssertionError("Expected _parse_dsl to reject conflicting methods")
