# datacube-benchmark

Benchmarking utilities for TiTiler-CMR and datacube tiling services.

## Installation

From the **titiler-cmr repository root**, install the package in editable mode:

```bash
uv pip install -e docs/packages/datacube_benchmark
```

Or with pip:

```bash
pip install -e docs/packages/datacube_benchmark
```

**Note**: The package must be installed before using it in notebooks. Once installed, it will be available in all notebooks in the project.

## Usage

```python
from datacube_benchmark import (
    DatasetParams,
    TiTilerCMRBenchmarker,
    benchmark_viewport,
    benchmark_tileset,
    benchmark_statistics,
    check_titiler_cmr_compatibility,
)

# Define dataset parameters
dataset = DatasetParams(
    concept_id="C2036881735-POCLOUD",
    backend="xarray",
    datetime_range="2022-03-01T00:00:01Z/2022-03-01T23:59:59Z",
    variable="analysed_sst",
    step="P1D",
    temporal_mode="point",
)

# Check compatibility
compat = await check_titiler_cmr_compatibility(
    endpoint="https://example.com/api/titiler-cmr",
    dataset=dataset,
)

# Benchmark viewport rendering
df_viewport = await benchmark_viewport(
    endpoint="https://example.com/api/titiler-cmr",
    dataset=dataset,
    lng=-95.0,
    lat=29.0,
    viewport_width=3,
    viewport_height=3,
    min_zoom=7,
    max_zoom=10,
)
```

## Components

- **DatasetParams**: Configuration dataclass for TiTiler-CMR datasets
- **TiTilerCMRBenchmarker**: Main benchmarking class for TiTiler-CMR
- **benchmark_viewport**: Benchmark tile rendering for viewport-like requests
- **benchmark_tileset**: Benchmark tile rendering for entire tilesets
- **benchmark_statistics**: Benchmark statistics endpoint performance
- **Utility functions**: Tile math, HTTP fetching, and result processing helpers
