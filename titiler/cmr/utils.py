"""titiler.cmr utilities.

Code from titiler.pgstac, MIT License.

"""

import logging
import time
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

import earthaccess
from geojson_pydantic import Feature, FeatureCollection
from isodate import parse_datetime as _parse_datetime
from rasterio.features import bounds
from rasterio.warp import transform_bounds
from urllib3.response import HTTPException

from titiler.cmr.errors import InvalidDatetime

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    # During development, enable proper type checking and code completion for
    # CRS and WGS84_CRS.
    from pyproj import CRS

    WGS84_CRS = CRS.from_epsg(4326)
else:
    from rasterio.crs import CRS
    from rio_tiler.constants import WGS84_CRS


def retry(
    tries: int,
    exceptions: Union[Type[Exception], Sequence[Type[Exception]]] = Exception,
    delay: float = 0.0,
):
    """Retry Decorator"""

    def _decorator(func: Any):
        def _newfn(*args: Any, **kwargs: Any):
            attempt = 0
            while attempt < tries:
                try:
                    return func(*args, **kwargs)

                except exceptions:  # type: ignore
                    attempt += 1
                    time.sleep(delay)

            return func(*args, **kwargs)

        return _newfn

    return _decorator


def _parse_date(date: str) -> datetime:
    try:
        return _parse_datetime(date)
    except Exception as e:
        raise InvalidDatetime(f"Invalid datetime {date}") from e


def parse_datetime(
    datetime_str: str,
) -> Tuple[Optional[datetime], Optional[datetime], Optional[datetime]]:
    """Parse datetime string input into datetime objects"""
    datetime_, start, end = None, None, None
    dt = datetime_str.split("/")
    if len(dt) == 1:
        datetime_ = _parse_date(dt[0])

    elif len(dt) == 2:
        dates: List[Optional[str]] = [None, None]
        dates[0] = dt[0] if dt[0] not in ["..", ""] else None
        dates[1] = dt[1] if dt[1] not in ["..", ""] else None

        if dates[0]:
            start = _parse_date(dates[0])

        if dates[1]:
            end = _parse_date(dates[1])

    else:
        raise InvalidDatetime("Invalid datetime: {datetime}")

    return datetime_, start, end


def get_concept_id_umm(concept_id: str) -> Dict[str, Any]:
    """Query CMR for the metadata for a concept_id"""
    results = earthaccess.collection_query().concept_id(concept_id).get(1)

    if not results:
        raise HTTPException(400, f"concept_id {concept_id} not found")

    return results[0]


def get_resolution_degrees(concept_id: str) -> Tuple[Optional[float], Optional[float]]:
    """Query CMR to get the resolution of a dataset using its concept_id. If the units are in meters
    convert to degrees using the rough conversion factor of 0.00001 degrees per meter"""
    ds = get_concept_id_umm(concept_id)

    try:
        resolution_info = ds["umm"]["SpatialExtent"]["HorizontalSpatialDomain"][
            "ResolutionAndCoordinateSystem"
        ]["HorizontalDataResolution"]["GenericResolutions"][0]
    except KeyError:
        logger.warning(
            f"could not find HorizontalDataResolution for concept_id {concept_id}"
        )
        return (None, None)

    units = resolution_info["Unit"].lower()
    if units not in ["meters", "decimal degrees"]:
        raise ValueError(
            f"cannot convert the coordinate units for concept_id {concept_id}: {units}"
        )

    conversion_factor = 0.00001 if units == "meters" else 1

    return (
        resolution_info["XDimension"] * conversion_factor,
        resolution_info["YDimension"] * conversion_factor,
    )


def get_bbox_degrees(
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    coord_crs: CRS | None = WGS84_CRS,
) -> tuple[float, float, float, float]:
    """Convert bounding box coordinates to WGS84 decimal degrees.

    If ``coord_crs`` is `None` or `WGS84_CRS`, the input coordinates are
    returned unchanged.  In the case of `None`, it is assumed that the inputs
    are already in WGS84 coordinates; no validation is performed.

    Parameters
    ----------
    minx
        Minimum X coordinate of the input bounding box.
    miny
        Minimum Y coordinate of the input bounding box.
    maxx
        Maximum X coordinate of the input bounding box.
    maxy
        Maximum Y coordinate of the input bounding box.
    coord_crs
        Coordinate reference system of the input bounding box. If ``None`` or
        equal to WGS84, no transformation is applied.

    Returns
    -------
    tuple[float, float, float, float]
        Bounding box coordinates expressed in WGS84 degrees as
        `(minx, miny, maxx, maxy)`.

    Examples
    --------
    Specifying no CRS or specifying WGS84 simply results in the same bounding
    box as given:

    >>> import pyproj
    >>> WGS84_CRS = pyproj.CRS.from_epsg(4326)
    >>> get_bbox_degrees(-180, -90, 180, 90)
    (-180, -90, 180, 90)
    >>> get_bbox_degrees(-180, -90, 180, 90, WGS84_CRS)
    (-180, -90, 180, 90)

    Specifying a CRS other than WGS84 results in an appropriately transformed
    bounding box:

    >>> WEB_MERCATOR_CRS = pyproj.CRS.from_epsg(3857)
    >>> valencia_wm_bbox = (-55_660, 4_777_695, -27_830, 4_800_765)
    >>> get_bbox_degrees(*valencia_wm_bbox, WEB_MERCATOR_CRS)
    (-0.50, 39.39, -0.25, 39.55)
    """
    if coord_crs is not None and coord_crs != WGS84_CRS:
        return transform_bounds(coord_crs, WGS84_CRS, minx, miny, maxx, maxy)

    return minx, miny, maxx, maxy


def calculate_time_series_request_size(
    concept_id: str,
    n_time_steps: int,
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    coord_crs: CRS,
) -> float:
    """Calculate the approximate magnitude of a time series request expressed
    as a total number of pixels read across the entire time series
    """
    xres, yres = get_resolution_degrees(concept_id)
    if not (xres and yres):
        return 0

    minx, miny, maxx, maxy = get_bbox_degrees(minx, miny, maxx, maxy, coord_crs)

    n_pixels_per_request = (maxx - minx) / xres * (maxy - miny) / yres

    return n_pixels_per_request * n_time_steps


def get_geojson_bounds(
    geojson: Union[Feature, FeatureCollection],
) -> Tuple[float, float, float, float]:
    """Get the global bounding box for a geojson Feature or FeatureCollection"""
    fc = geojson
    if isinstance(fc, Feature):
        fc = FeatureCollection(type="FeatureCollection", features=[geojson])

    all_bounds = [
        bounds(feature.model_dump(exclude_none=True)) for feature in fc.features
    ]

    minx = min(bound[0] for bound in all_bounds)
    miny = min(bound[1] for bound in all_bounds)
    maxx = max(bound[2] for bound in all_bounds)
    maxy = max(bound[3] for bound in all_bounds)

    return (minx, miny, maxx, maxy)
