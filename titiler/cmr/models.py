"""titiler.cmr models."""

import os
import re
from collections import defaultdict
from typing import Annotated, Any, List

from fastapi import Query
from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator
from shapely.geometry import shape

from titiler.cmr.errors import S3CredentialsEndpointMissing

# ---------------------------------------------------------------------------
# Query parameter type aliases
# ---------------------------------------------------------------------------

ConceptID = Annotated[
    str | None,
    Query(
        description="""A CMR concept id, in the format <concept-type-prefix> <unique-number> '-' <provider-id>
- concept-type-prefix is a single capital letter prefix indicating the concept type. "C" is used for collections
- unique-number is a single number assigned by the CMR during ingest.
- provider-id is the short name for the provider. i.e. "LPDAAC_ECS"
        """
    ),
]

Temporal = Annotated[
    str | None,
    Query(
        description="""Filters items that have a temporal property that intersects this value.\n
Either a date-time or an interval, open or closed. Date and time expressions adhere to RFC 3339. Open intervals are expressed using double-dots.""",
        openapi_examples={
            "user-defined": {"value": None},
            "datetime": {"value": "2018-02-12T23:20:50Z"},
            "closed-interval": {"value": "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"},
            "open-interval-from": {"value": "2018-02-12T00:00:00Z/"},
            "open-interval-to": {"value": "/2018-03-18T12:31:12Z"},
        },
    ),
]

GranuleUr = Annotated[str | None, Query(description="Unique granule record id")]

CloudCover = Annotated[
    str | None,
    Query(
        description="Cloud cover range",
        openapi_examples={
            "user-provided": {"value": None},
            "max 20": {"value": "0,20"},
        },
    ),
]

BBox = Annotated[
    str | None,
    Query(
        description="bounding box coordinates",
        openapi_examples={
            "user-provided": {"value": None},
            "example": {"value": "-100,40,-90,50"},
        },
    ),
]

SortKey = Annotated[
    List[str] | None,
    Query(
        description=(
            "One or more sort keys for granule results. Prefix with `-` for descending order. "
            "Valid keys: campaign, entry_title, dataset_id, data_size, end_date, granule_ur, "
            "producer_granule_id, project, provider, readable_granule_name, short_name, "
            "start_date, version, platform, instrument, sensor, day_night_flag, online_only, "
            "browsable, cloud_cover, revision_date."
        ),
        openapi_examples={
            "most-recent-first": {"value": ["-start_date"]},
            "ascending-start-date": {"value": ["start_date"]},
            "multi-key": {"value": ["provider", "-start_date"]},
        },
    ),
]

# ---------------------------------------------------------------------------
# Granule search
# ---------------------------------------------------------------------------


class GranuleSearch(BaseModel):
    """CMR granule search parameters."""

    collection_concept_id: ConceptID = None
    granule_ur: GranuleUr = None
    temporal: Temporal = None
    cloud_cover: CloudCover = None
    bounding_box: BBox | None = None
    sort_key: List[str] | None = None

    @field_validator("temporal")
    @classmethod
    def normalize_temporal(cls, v: str | None) -> str | None:
        """Convert a singleton datetime to a closed interval.

        CMR interprets a bare datetime (e.g. "2024-01-01T00:00:00Z") as an
        open-ended range starting at that date. Convert it to a closed interval
        ("2024-01-01T00:00:00Z/2024-01-01T00:00:00Z") so only granules for
        that exact instant are matched.
        """
        if v is not None and "/" not in v:
            return f"{v}/{v}"
        return v


# ---------------------------------------------------------------------------
# Granule response models
# ---------------------------------------------------------------------------


class Asset(BaseModel):
    """A single CMR granule asset with direct (S3) and external (HTTPS) hrefs."""

    direct_href: str
    external_href: str
    ext: str


class Link(BaseModel):
    """A link entry from a CMR granule search result."""

    rel: str
    hreflang: str
    href: str
    title: str | None = None
    inherited: bool | None = None


def _parse_ring(ring_str: str) -> list[list[float]]:
    """Parse a CMR ring string into GeoJSON coordinates.

    CMR encodes rings as a flat string of space-separated lat/lon pairs.
    GeoJSON requires [lon, lat] order, so the values are swapped.
    """
    values = [float(v) for v in ring_str.split()]
    return [[values[i + 1], values[i]] for i in range(0, len(values), 2)]


def _parse_box(box_str: str) -> list[list[float]]:
    """Parse a CMR bounding box string into a closed GeoJSON polygon ring.

    CMR encodes boxes as "south west north east" (lat/lon order).
    Returns a closed ring in GeoJSON [lon, lat] order.
    """
    south, west, north, east = [float(v) for v in box_str.split()]
    return [
        [west, south],
        [east, south],
        [east, north],
        [west, north],
        [west, south],
    ]


class Granule(BaseModel):
    """A single CMR granule parsed from a search response."""

    id: str
    collection_concept_id: str
    links: list[Link]
    polygons: list[list[str]] | None = None
    boxes: list[str] | None = None
    time_start: str | None = None
    time_end: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def geometry(self) -> dict[str, Any] | None:
        """GeoJSON geometry derived from the CMR polygons or boxes field."""
        if self.polygons:
            rings_per_polygon = [
                [_parse_ring(ring) for ring in polygon] for polygon in self.polygons
            ]
            if len(rings_per_polygon) == 1:
                return {"type": "Polygon", "coordinates": rings_per_polygon[0]}
            return {"type": "MultiPolygon", "coordinates": rings_per_polygon}

        if self.boxes:
            rings = [_parse_box(box) for box in self.boxes]
            if len(rings) == 1:
                return {"type": "Polygon", "coordinates": [rings[0]]}
            return {"type": "MultiPolygon", "coordinates": [[ring] for ring in rings]}

        return None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def bbox(self) -> tuple[float, float, float, float]:
        """Bounding box derived from the granule geometry."""
        return shape(self.geometry).bounds

    @computed_field  # type: ignore[prop-decorator]
    @property
    def s3_credentials_endpoint(self) -> str:
        """S3 credentials endpoint URL from the granule links."""
        endpoint = next(
            (link.href for link in self.links if "/s3credentials" in link.href), None
        )
        if not endpoint:
            raise S3CredentialsEndpointMissing(
                f"granule ({self.id}) does not have an s3 credentials link"
            )
        return endpoint

    def get_assets(self, regex: str | None = None) -> dict[str, Asset]:
        """Extract assets from granule links, optionally filtered by regex."""
        _assets: dict[str, dict] = defaultdict(dict)
        for link in self.links:
            root, extension = os.path.splitext(link.href)
            file = root.split("/")[-1]

            if regex:
                if match := re.search(regex, file):
                    key = match.group()
                else:
                    continue
            else:
                key = file

            if not link.inherited:
                if link.rel.endswith("/s3#"):
                    _assets[key]["ext"] = extension
                    _assets[key]["direct_href"] = link.href
                elif link.rel.endswith("/data#"):
                    _assets[key]["external_href"] = link.href

        return {
            (key if regex else str(i)): Asset(**data)
            for i, (key, data) in enumerate(_assets.items())
        }


class GranuleSearchFeed(BaseModel):
    """The feed wrapper in a CMR granule search response."""

    entry: list[Granule]


class GranuleSearchResponse(BaseModel):
    """Top-level CMR granules.json search response."""

    feed: GranuleSearchFeed


# ---------------------------------------------------------------------------
# Collection UMM models
# ---------------------------------------------------------------------------


class GenericResolution(BaseModel):
    """A single horizontal resolution entry from UMM metadata."""

    model_config = ConfigDict(populate_by_name=True)

    x_dimension: float = Field(alias="XDimension")
    y_dimension: float = Field(alias="YDimension")
    unit: str = Field(alias="Unit")


class HorizontalDataResolution(BaseModel):
    """UMM HorizontalDataResolution container."""

    model_config = ConfigDict(populate_by_name=True)

    generic_resolutions: list[GenericResolution] | None = Field(
        None, alias="GenericResolutions"
    )


class ResolutionAndCoordinateSystem(BaseModel):
    """UMM ResolutionAndCoordinateSystem container."""

    model_config = ConfigDict(populate_by_name=True)

    horizontal_data_resolution: HorizontalDataResolution | None = Field(
        None, alias="HorizontalDataResolution"
    )


class HorizontalSpatialDomain(BaseModel):
    """UMM HorizontalSpatialDomain container."""

    model_config = ConfigDict(populate_by_name=True)

    resolution_and_coordinate_system: ResolutionAndCoordinateSystem | None = Field(
        None, alias="ResolutionAndCoordinateSystem"
    )


class CollectionSpatialExtent(BaseModel):
    """UMM SpatialExtent container."""

    model_config = ConfigDict(populate_by_name=True)

    horizontal_spatial_domain: HorizontalSpatialDomain | None = Field(
        None, alias="HorizontalSpatialDomain"
    )


class Collection(BaseModel):
    """A CMR collection parsed from the UMM-JSON search response."""

    model_config = ConfigDict(populate_by_name=True)

    temporal_extents: list[Any] = Field(alias="TemporalExtents")
    spatial_extent: CollectionSpatialExtent | None = Field(None, alias="SpatialExtent")

    @property
    def resolution_degrees(self) -> tuple[float | None, float | None]:
        """Return (x_res, y_res) in decimal degrees, or (None, None) if unavailable.

        Meters are converted to degrees using the factor 0.00001 deg/m.
        Raises ValueError if the unit is not meters or decimal degrees.
        """
        if not (
            (se := self.spatial_extent)
            and (hsd := se.horizontal_spatial_domain)
            and (rcs := hsd.resolution_and_coordinate_system)
            and (hdr := rcs.horizontal_data_resolution)
            and (gr := hdr.generic_resolutions)
        ):
            return (None, None)

        resolution_info = gr[0]

        units = resolution_info.unit.lower()
        if units not in ("meters", "decimal degrees"):
            raise ValueError(f"cannot convert coordinate units: {units}")

        factor = 0.00001 if units == "meters" else 1
        return (
            resolution_info.x_dimension * factor,
            resolution_info.y_dimension * factor,
        )


class CollectionItem(BaseModel):
    """A single item in a UMM-JSON collection search response."""

    umm: Collection


class CollectionSearchResponse(BaseModel):
    """Top-level CMR collections.umm_json response."""

    items: list[CollectionItem]
