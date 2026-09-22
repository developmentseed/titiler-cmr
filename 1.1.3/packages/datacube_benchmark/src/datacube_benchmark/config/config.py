"""
datacube_benchmark.config
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple


# ------------------------------
# Dataclasses
# ------------------------------
from typing import Optional


@dataclass
class DatasetParams:
    """
    Encapsulates parameters for requesting tiles from TiTiler-CMR.

    Required:
        concept_id (str): CMR concept ID for the dataset.
        backend (str): Backend type, e.g., "xarray" or "rasterio".
        datetime_range (str): ISO8601 interval, e.g., "2024-10-01T00:00:00Z/2024-10-10T00:00:00Z".

    Optional (backend-dependent):
        variable (str): For xarray backend, the variable name.
        bands (Sequence[str]): For rasterio backend, list of bands.
        bands_regex (str): For rasterio backend, regex for band selection.
        rescale (str): Rescale range for visualization.
        colormap_name (str): Colormap name for visualization.
        resampling (str): Resampling method.
        step (str): Temporal stepping, e.g., "P1D".
        temporal_mode (str): Temporal aggregation mode, e.g., "point".
        minzoom (int): Minimum zoom level.
        maxzoom (int): Maximum zoom level.
        tile_format (str): Output tile format.
        tile_scale (int): Tile scaling factor.
        **others**: Extend as needed.

    Raises:
        ValueError: If required backend-specific fields are missing.
    """

    concept_id: str
    backend: str
    datetime_range: str

    # Xarray
    variable: Optional[str] = None

    # Rasterio
    bands: Optional[Sequence[str]] = None
    bands_regex: Optional[str] = None

    # Common optional params
    rescale: Optional[str] = None
    colormap_name: Optional[str] = None
    resampling: Optional[str] = None
    step: Optional[str] = None
    temporal_mode: Optional[str] = None
    minzoom: Optional[int] = None
    maxzoom: Optional[int] = None
    tile_format: Optional[str] = None
    tile_scale: Optional[int] = None

    def to_query_params(self, **extra_kwargs: Any) -> List[Tuple[str, str]]:  # noqa: C901
        """
        Convert dataset parameters into query parameters for TiTiler-CMR.

        Combines required fields and all additional keyword arguments, filtering
        out None values and converting types as needed.

        Raises:
            ValueError: If required backend-specific fields are missing.
        """
        params: List[Tuple[str, str]] = [
            ("concept_id", self.concept_id),
            ("backend", self.backend),
            ("datetime", self.datetime_range),
        ]

        # Backend-specific validation
        if self.backend == "xarray":
            if not self.variable:
                raise ValueError("For backend='xarray', 'variable' must be provided.")
        elif self.backend == "rasterio":
            if not (self.bands and self.bands_regex):
                raise ValueError(
                    "For backend='rasterio', 'bands' and 'bands_regex' must be provided."
                )

        # Collect from dataclass fields
        all_kwargs: Dict[str, Any] = {
            "variable": self.variable,
            "bands": self.bands,
            "bands_regex": self.bands_regex,
            "rescale": self.rescale,
            "colormap_name": self.colormap_name,
            "resampling": self.resampling,
            "step": self.step,
            "temporal_mode": self.temporal_mode,
            "minzoom": self.minzoom,
            "maxzoom": self.maxzoom,
            "tile_format": self.tile_format,
            "tile_scale": self.tile_scale,
        }
        all_kwargs.update(extra_kwargs)

        for k, v in all_kwargs.items():
            if v is None:
                continue
            if isinstance(v, bool):
                params.append((k, "true" if v else "false"))
            elif isinstance(v, (int, float)):
                params.append((k, str(v)))
            elif isinstance(v, (list, tuple, set)):
                for item in v:
                    if item is not None:
                        params.append((k, str(item)))
            elif isinstance(v, str):
                params.append((k, v))
            else:
                print(f"Unexpected type for param '{k}': {type(v)}. Value: {v}")
        return params
