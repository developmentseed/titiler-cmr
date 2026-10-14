"""Tests for CMR Pydantic models."""

from titiler.cmr.models import CollectionSearchResponse


def test_collection_resolution_allows_partial_generic_resolution() -> None:
    """Collection UMM metadata can omit one resolution dimension."""
    response = CollectionSearchResponse.model_validate(
        {
            "items": [
                {
                    "umm": {
                        "TemporalExtents": [],
                        "SpatialExtent": {
                            "HorizontalSpatialDomain": {
                                "ResolutionAndCoordinateSystem": {
                                    "HorizontalDataResolution": {
                                        "GenericResolutions": [
                                            {"YDimension": 250, "Unit": "Not provided"}
                                        ]
                                    }
                                }
                            }
                        },
                    }
                }
            ]
        }
    )

    assert response.items[0].umm.resolution_degrees == (None, None)
