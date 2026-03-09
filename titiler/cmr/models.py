"""titiler.cmr models."""

import os
import re
from collections import defaultdict
from typing import Annotated, Any, List

from fastapi import Query
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
    model_validator,
)
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

    model_config = ConfigDict(populate_by_name=True)

    collection_concept_id: ConceptID = Field(
        default=None,
        validation_alias=AliasChoices("collection_concept_id", "concept_id"),
    )
    granule_ur: GranuleUr = None
    temporal: Temporal = Field(
        default=None,
        validation_alias=AliasChoices("temporal", "datetime"),
    )
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


class RelatedUrl(BaseModel):
    """A UMM RelatedUrl entry from a CMR granule search result."""

    model_config = ConfigDict(populate_by_name=True)

    url: str = Field(alias="URL")
    type: str = Field(alias="Type")
    description: str | None = Field(None, alias="Description")


class GPolygonPoint(BaseModel):
    """A single point in a UMM GPolygon boundary."""

    model_config = ConfigDict(populate_by_name=True)

    longitude: float = Field(alias="Longitude")
    latitude: float = Field(alias="Latitude")


class GPolygonBoundary(BaseModel):
    """The boundary of a UMM GPolygon."""

    model_config = ConfigDict(populate_by_name=True)

    points: list[GPolygonPoint] = Field(alias="Points")


class GPolygon(BaseModel):
    """A UMM GPolygon geometry."""

    model_config = ConfigDict(populate_by_name=True)

    boundary: GPolygonBoundary = Field(alias="Boundary")


class BoundingRectangle(BaseModel):
    """A UMM BoundingRectangle geometry."""

    model_config = ConfigDict(populate_by_name=True)

    west: float = Field(alias="WestBoundingCoordinate")
    east: float = Field(alias="EastBoundingCoordinate")
    north: float = Field(alias="NorthBoundingCoordinate")
    south: float = Field(alias="SouthBoundingCoordinate")


class UMMGeometry(BaseModel):
    """UMM Geometry container with polygons and/or bounding rectangles."""

    model_config = ConfigDict(populate_by_name=True)

    g_polygons: list[GPolygon] | None = Field(None, alias="GPolygons")
    bounding_rectangles: list[BoundingRectangle] | None = Field(
        None, alias="BoundingRectangles"
    )


class GranuleHorizontalSpatialDomain(BaseModel):
    """UMM HorizontalSpatialDomain container for granules."""

    model_config = ConfigDict(populate_by_name=True)

    geometry: UMMGeometry | None = Field(None, alias="Geometry")


class GranuleSpatialExtent(BaseModel):
    """UMM SpatialExtent container for granules."""

    model_config = ConfigDict(populate_by_name=True)

    horizontal_spatial_domain: GranuleHorizontalSpatialDomain | None = Field(
        None, alias="HorizontalSpatialDomain"
    )


class RangeDateTime(BaseModel):
    """UMM RangeDateTime container."""

    model_config = ConfigDict(populate_by_name=True)

    beginning_date_time: str | None = Field(None, alias="BeginningDateTime")
    ending_date_time: str | None = Field(None, alias="EndingDateTime")


class GranuleTemporalExtent(BaseModel):
    """UMM TemporalExtent container for granules."""

    model_config = ConfigDict(populate_by_name=True)

    range_date_time: RangeDateTime | None = Field(None, alias="RangeDateTime")


class AdditionalAttribute(BaseModel):
    """A UMM AdditionalAttribute entry."""

    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(alias="Name")
    values: list[str] = Field(alias="Values")


class ArchiveAndDistributionInfo(BaseModel):
    """A UMM ArchiveAndDistributionInformation entry."""

    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(alias="Name")
    size_in_bytes: int | None = Field(None, alias="SizeInBytes")


class DataGranule(BaseModel):
    """UMM DataGranule container."""

    model_config = ConfigDict(populate_by_name=True)

    archive_and_distribution_information: list[ArchiveAndDistributionInfo] = Field(
        alias="ArchiveAndDistributionInformation", default_factory=list
    )


class Granule(BaseModel):
    """A single CMR granule parsed from a UMM JSON search response."""

    id: str
    collection_concept_id: str
    related_urls: list[RelatedUrl] = Field(default_factory=list)
    spatial_extent: GranuleSpatialExtent | None = None
    temporal_extent: GranuleTemporalExtent | None = None
    additional_attributes: list[AdditionalAttribute] = Field(default_factory=list)
    data_granule: DataGranule | None = None

    @model_validator(mode="before")
    @classmethod
    def _extract_from_umm_item(cls, data: Any) -> Any:
        """Extract flat fields from a raw UMM item dict {"meta": ..., "umm": ...}."""
        if isinstance(data, dict) and "meta" in data and "umm" in data:
            meta = data["meta"]
            umm = data["umm"]
            return {
                "id": meta["concept-id"],
                "collection_concept_id": meta["collection-concept-id"],
                "related_urls": umm.get("RelatedUrls", []),
                "spatial_extent": umm.get("SpatialExtent"),
                "temporal_extent": umm.get("TemporalExtent"),
                "additional_attributes": umm.get("AdditionalAttributes", []),
                "data_granule": umm.get("DataGranule"),
            }
        return data

    @property
    def additional_attributes_dict(self) -> dict[str, list[str]]:
        """Return additional attributes as a name→values dict."""
        return {attr.name: attr.values for attr in self.additional_attributes}

    @computed_field  # type: ignore[prop-decorator]
    @property
    def geometry(self) -> dict[str, Any] | None:
        """GeoJSON geometry derived from UMM SpatialExtent."""
        if not (
            self.spatial_extent
            and (hsd := self.spatial_extent.horizontal_spatial_domain)
            and (geom := hsd.geometry)
        ):
            return None

        if geom.g_polygons:
            polygons = []
            for polygon in geom.g_polygons:
                ring = [[pt.longitude, pt.latitude] for pt in polygon.boundary.points]
                # Ensure ring is closed
                if ring and ring[0] != ring[-1]:
                    ring.append(ring[0])
                polygons.append([ring])
            if len(polygons) == 1:
                return {"type": "Polygon", "coordinates": polygons[0]}
            return {"type": "MultiPolygon", "coordinates": polygons}

        if geom.bounding_rectangles:
            rings = []
            for br in geom.bounding_rectangles:
                ring = [
                    [br.west, br.south],
                    [br.east, br.south],
                    [br.east, br.north],
                    [br.west, br.north],
                    [br.west, br.south],
                ]
                rings.append(ring)
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
        """S3 credentials endpoint URL from the granule related URLs."""
        endpoint = next(
            (ru.url for ru in self.related_urls if "/s3credentials" in ru.url), None
        )
        if not endpoint:
            raise S3CredentialsEndpointMissing(
                f"granule ({self.id}) does not have an s3 credentials link"
            )
        return endpoint

    def get_assets(self, regex: str | None = None) -> dict[str, Asset]:  # noqa: C901
        """Extract assets from granule related URLs, optionally filtered by regex."""
        _assets: dict[str, dict] = defaultdict(dict)

        # Restrict to canonical filenames from DataGranule when available
        canonical_names: set[str] | None = None
        if self.data_granule:
            names = {
                info.name
                for info in self.data_granule.archive_and_distribution_information
                if info.name != "Not provided"
            }
            if names:
                canonical_names = names

        for ru in self.related_urls:
            if ru.type not in ("GET DATA VIA DIRECT ACCESS", "GET DATA"):
                continue

            root, extension = os.path.splitext(ru.url)
            file = root.split("/")[-1]

            if canonical_names is not None:
                if (file + extension) not in canonical_names:
                    continue

            if regex:
                if match := re.search(regex, file):
                    key = match.group()
                else:
                    continue
            else:
                key = file

            if ru.type == "GET DATA VIA DIRECT ACCESS":
                _assets[key]["ext"] = extension
                _assets[key]["direct_href"] = ru.url
            elif ru.type == "GET DATA":
                _assets[key]["external_href"] = ru.url

        return {
            (key if regex else str(i)): Asset(**data)
            for i, (key, data) in enumerate(_assets.items())
        }


class GranuleSearchResponse(BaseModel):
    """Top-level CMR granules.umm_json search response."""

    model_config = ConfigDict(extra="ignore")

    hits: int
    items: list[Granule]


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
