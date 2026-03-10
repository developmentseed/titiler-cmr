# Architecture

## Overview

`titiler-cmr` sits on top of `titiler.mosaic`, which provides a `MosaicTilerFactory` pattern. In this pattern a *backend* orchestrates multi-source search and tiling while a *reader* handles per-source data access. `CMRTilerFactory` extends `MosaicTilerFactory` with two backend variants that correspond to different data-access paths:

- **Rasterio** — for granules that expose georeferenced raster assets (GeoTIFF, COG, etc.), accessed via `rasterio`.
- **Xarray** — for granules that expose NetCDF/HDF5 files, opened via `xarray` and `open_dataset`.

Each variant is registered as a separate router (e.g. `/rasterio/tiles/…` and `/xarray/tiles/…`) but shares the same `CMRBackend` for granule search. The reader class and its dependency are swapped to handle the format-specific opening logic.

---

## Class Hierarchy

```
rio_tiler.io.base.BaseReader          rio_tiler.io.base.SpatialMixin
   │                                          │
   ├── rio_tiler.io.xarray.XarrayReader        ├── rio_tiler.io.base.MultiBaseReader
   │       │                                   │         │
   │       └── XarrayGranuleReader             │         └── MultiBaseGranuleReader
   │                                           │
   └── rio_tiler.mosaic.backend.BaseBackend
           │
           └── CMRBackend

titiler.core.factory.BaseFactory
   │
   └── titiler.mosaic.factory.MosaicTilerFactory
           │
           └── CMRTilerFactory
```

`CMRBackend` is instantiated once per request. It receives the granule search parameters and holds a reference to whichever reader class (`XarrayGranuleReader` or `MultiBaseGranuleReader`) was configured at factory registration time.

---

## Dependency Injection Flow

`CMRTilerFactory` uses FastAPI dependency injection to assemble the backend and reader for each request. The table below maps each factory field to its concrete class for each backend variant and where the resolved value ends up.

| Factory field | Concrete class (xarray) | Concrete class (rasterio) | Where it flows |
|---|---|---|---|
| `path_dependency` | `GranuleSearchParams` | `GranuleSearchParams` | `CMRBackend.input` (a `GranuleSearch`) |
| `backend_dependency` | `BackendParams` | `BackendParams` | `CMRBackend` kwargs (`client`, `auth_token`, `s3_access`, `get_s3_credentials`) |
| `reader_dependency` | `XarrayParams` | `CMRAssetsParams` | `CMRBackend.reader_options` (merged, then splatted into reader constructor) |
| `assets_accessor_dependency` | `GranuleSearchBackendParams` | `GranuleSearchBackendParams` | `BaseBackend.tile(search_options=…)` — controls granule search behaviour |
| `dataset_dependency` | `XarrayDatasetParams` | `RasterioDatasetParams` | reader method call kwargs (`.tile()`, `.part()`, etc.) |
| `layer_dependency` | `ExpressionParams` | `AssetsExprParams` | reader method call kwargs (band indexes / expressions / assets) |

---

## Request Flow for a Tile Endpoint

The following trace follows a request through the xarray backend. The rasterio equivalent is described below.

```
GET /xarray/tiles/WebMercatorQuad/{z}/{x}/{y}?collection_concept_id=...&variable=sst

1. FastAPI resolves dependencies:
   - GranuleSearchParams        → GranuleSearch(collection_concept_id=...)
   - BackendParams              → {client, auth_token, s3_access}  (from app.state)
   - XarrayParams               → {variable="sst", group=None, ...}
   - GranuleSearchBackendParams → {items_limit, exitwhenfull, skipcovered}
   - XarrayDatasetParams        → {nodata, reproject_method}

2. MosaicTilerFactory.tile() opens the backend:
   CMRBackend(
     input=GranuleSearch,
     reader=XarrayGranuleReader,
     reader_options={"variable": "sst", ...},                          ← from XarrayParams
     client=..., auth_token=..., s3_access=..., get_s3_credentials=... ← from BackendParams
   )

3. CMRBackend.__attrs_post_init__ merges auth_token, s3_access, and get_s3_credentials into reader_options:
   reader_options = {"variable": "sst", "auth_token": "...", "s3_access": False, "get_s3_credentials": ...}

4. BaseBackend.tile(x, y, z, search_options={...}) runs:
   a. CMRBackend.assets_for_tile(x, y, z, exitwhenfull=True)
      → queries CMR API → returns [Granule, Granule, ...]

   b. For each Granule:
      XarrayGranuleReader(granule, tms=tms, **reader_options)

5. XarrayGranuleReader.__attrs_post_init__:
   a. Calls granule.get_assets() → asset dict keyed "0", "1", ...
   b. Selects asset["0"], resolves href (direct_href vs external_href)
   c. Calls open_dataset(href, group=..., decode_times=..., auth_token=...)
   d. Calls get_variable(ds, variable="sst", sel=...) → xarray.DataArray
   e. Sets self.input = DataArray
   f. Calls super().__attrs_post_init__() → rio_tiler.XarrayReader sets bounds, CRS, etc.

6. src_dst.tile(x, y, z, **dataset_params) → ImageData

7. mosaic_reader merges N ImageData arrays using the configured pixel_selection method

8. Post-process (algorithm) → render → Response(bytes)
```

For the **rasterio** backend, steps 2–6 are replaced by `MultiBaseGranuleReader`. It discovers the list of assets from the `Granule` at instantiation time and, for each asset, dispatches to a rasterio `Reader` (for COG/GeoTIFF) or `XarrayReader` (for NetCDF). `MultiBaseReader` handles iterating assets, merging per-asset results, and exposing them with index/expression filtering.

---

## Why Two Reader Base Classes?

The two reader classes diverge at the level of what a single CMR granule represents:

**`MultiBaseGranuleReader(MultiBaseReader)`**

Used when a single CMR granule may contain *multiple* assets — for example, one GeoTIFF per spectral band. `MultiBaseReader` is designed for this pattern: it holds a list of asset URLs, iterates them, merges results, and exposes them through band-index and expression filtering. The granule's asset list is resolved at instantiation time by calling `granule.get_assets()`.

**`XarrayGranuleReader(rio_tiler.io.xarray.XarrayReader)`**

Used when a CMR granule is a single NetCDF/HDF5 file containing one or more variables. This class extends the *low-level* `rio_tiler.io.xarray.XarrayReader`, which expects a pre-built `xarray.DataArray` as its `input` attribute, rather than `titiler.xarray.io.Reader`, which owns `src_path: str` and handles opening internally with a fixed opener signature.

The reason for choosing the lower-level base: `XarrayGranuleReader` must accept a `Granule` object (not a plain path), extract the correct href (`direct_href` vs `external_href` depending on `s3_access`), and invoke the CMR-specific `open_dataset` with authentication. By controlling the full opening pipeline in `__attrs_post_init__` and then setting `self.input` before calling `super().__attrs_post_init__()`, `XarrayGranuleReader` fits naturally into the rio_tiler reader contract without fighting the assumptions baked into `titiler.xarray.io.Reader`.

---

## S3 Credential Handling

NASA DAAC data in S3 requires temporary credentials obtained from a per-DAAC endpoint, not
long-lived IAM keys. Earthdata Login provides a bearer token that can be exchanged for
short-lived `(access_key_id, secret_access_key, session_token)` credentials scoped to that DAAC's
bucket. The credential machinery in `titiler-cmr` is split across three layers: startup,
app-state caching, and per-granule provider caching.

### Startup

`startup()` in `main.py` runs once at application boot (or Lambda warm start):

1. If `EARTHDATA_USERNAME` and `EARTHDATA_PASSWORD` are set, a bearer token is obtained from
   `https://urs.earthdata.nasa.gov/api/users/find_or_create_token` and stored at
   `app.state.earthdata_token`.
2. If `EARTHDATA_S3_DIRECT_ACCESS=true`, `make_get_s3_credentials(auth_token)` is called and
   the resulting factory callable is stored at `app.state.get_s3_credentials`.

`make_get_s3_credentials` returns a function decorated with a `TTLCache(maxsize=100, ttl=50m)`.
Calling it with an endpoint URL either constructs a new `EarthdataS3CredentialProvider` or
returns the cached instance for that endpoint. The 50-minute TTL covers the expected lifetime of
a Lambda execution environment and prevents redundant provider construction across requests.

### Per-request propagation

`BackendParams` is a FastAPI dependency that runs on every request. It reads
`app.state.{earthdata_token, s3_access, get_s3_credentials}` and passes them into
`CMRBackend`. `CMRBackend.__attrs_post_init__` merges them into `reader_options`, so every
reader instance receives them as constructor arguments.

### Per-granule credential provider (`EarthdataS3CredentialProvider`)

Each reader calls `granule.s3_credentials_endpoint` during `__attrs_post_init__`. This property
scans the granule's `related_urls` for a URL containing `/s3credentials` and raises
`S3CredentialsEndpointMissing` if none is found (different DAACs expose different URLs). The
reader then calls `get_s3_credentials(endpoint)` to retrieve the cached
`EarthdataS3CredentialProvider` instance for that endpoint.

`EarthdataS3CredentialProvider` is a callable that returns `S3Credential`. When called it
checks whether the cached credentials expire within 5 minutes (`CREDENTIAL_REFRESH_BUFFER`).
If so (or on the first call), it fetches new credentials from the endpoint using the bearer
token and caches the result. A `threading.Lock` makes it safe to share a single provider
instance across concurrent reads within the same process.

The credentials are used differently by each backend:

- **Rasterio** (`MultiBaseGranuleReader`): `_get_asset_info` calls the provider on each asset,
  constructs an `AWSSession` from the returned keys, and injects it into the rasterio `Env`
  via `AssetInfo.env`.
- **Xarray** (`XarrayGranuleReader`): the provider callable itself is passed as
  `credential_provider` to `obstore`, which calls it whenever it needs to refresh credentials
  during streaming reads.

### Fallback behaviour

If `s3_access` is `False`, or if the granule has no `/s3credentials` URL, each reader falls
back to the HTTPS asset URL (`asset.external_href`) and, if a bearer token is present, attaches
it as an `Authorization: Bearer <token>` header. In this path no S3 credentials are requested
or used.
