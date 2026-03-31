# Architecture

## Overview

`titiler-cmr` sits on top of `titiler.mosaic`, which provides a `MosaicTilerFactory` pattern. TiTiler's object-oriented design includes 2 foundational class types: *backends* and *readers*. In this pattern a *backend* orchestrates multi-source search and tiling while a *reader* handles per-source data access. `CMRTilerFactory` extends `MosaicTilerFactory` with two backend variants that correspond to different data-access paths:

- **rasterio** — for granules that expose georeferenced raster assets (GeoTIFF, COG, etc.), accessed via `rasterio`.
- **xarray** — for granules that expose NetCDF/HDF5 files, opened via `xarray` and `open_dataset`.

Each variant is registered as a separate router (e.g. `/rasterio/tiles/…` and `/xarray/tiles/…`) but shares the same `CMRBackend` for granule search. The reader class and its dependency are swapped to handle the format-specific opening logic.

---

## Class Hierarchy

```
rio_tiler.io.base.SpatialMixin
   │
   ├── rio_tiler.io.base.BaseReader
   │       │
   │       ├── rio_tiler.io.xarray.XarrayReader
   │       │       │
   │       │       └── XarrayGranuleReader
   │       │
   │       └── rio_tiler.mosaic.backend.BaseBackend
   │               │
   │               └── CMRBackend
   │
   └── rio_tiler.io.base.MultiBaseReader
           │
           └── MultiBaseGranuleReader

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
| `reader_dependency` | `interpolated_xarray_ds_params` | `CMRAssetsParams` | `CMRBackend.reader_options` (merged, then splatted into reader constructor) |
| `assets_accessor_dependency` | `GranuleSearchBackendParams` | `RasterioGranuleSearchBackendParams` | `BaseBackend.tile(search_options=…)` — controls granule search behaviour |
| `dataset_dependency` | `XarrayDatasetParams` | `RasterioDatasetParams` | reader method call kwargs (`.tile()`, `.part()`, etc.) |
| `layer_dependency` | `ExpressionParams` | `CMRAssetsExprParams` | reader method call kwargs (band indexes / expressions / assets) |

---

## Request Flow for a Tile Endpoint

The following trace follows a request through the xarray backend. The rasterio equivalent is described below.

```
GET /xarray/tiles/WebMercatorQuad/{z}/{x}/{y}?collection_concept_id=...&variables=sst

1. FastAPI resolves dependencies:
   - GranuleSearchParams            → GranuleSearch(collection_concept_id=...)
   - BackendParams                  → {client, auth_token, s3_access}  (from app.state)
   - interpolated_xarray_ds_params  → {variables=["sst"], group=None, ...}
   - GranuleSearchBackendParams     → {items_limit, exitwhenfull, skipcovered}
   - XarrayDatasetParams            → {nodata, reproject_method}

2. MosaicTilerFactory.tile() opens the backend:
   CMRBackend(
     input=GranuleSearch,
     reader=XarrayGranuleReader,
     reader_options={"variables": ["sst"], ...},                       ← from interpolated_xarray_ds_params
     client=..., auth_token=..., s3_access=..., get_s3_credentials=... ← from BackendParams
   )

3. CMRBackend.__attrs_post_init__ merges auth_token, s3_access, and get_s3_credentials into reader_options:
   reader_options = {"variables": ["sst"], "auth_token": "...", "s3_access": False, "get_s3_credentials": ...}

4. BaseBackend.tile(x, y, z, search_options={...}) runs:
   a. CMRBackend.assets_for_tile(x, y, z, exitwhenfull=True)
      → queries CMR API → returns [Granule, Granule, ...]

   b. For each Granule:
      XarrayGranuleReader(granule, tms=tms, **reader_options)

5. XarrayGranuleReader.__attrs_post_init__:
   a. Calls granule.get_assets() → asset dict keyed "0", "1", ...
   b. Selects asset["0"], resolves href (direct_href vs external_href)
   c. Calls open_dataset(href, group=..., decode_times=..., auth_token=...)
   d. Calls get_variables(ds, variables=["sst"], sel=...) → xarray.DataArray
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
long-lived IAM keys. The credential machinery in `titiler-cmr` is split across three layers:
startup, app-state caching, and per-granule provider caching.

### Startup

`startup()` in `main.py` runs once at application boot (or Lambda warm start). It initialises
`app.state.s3_access`, `app.state.earthdata_token_provider`, and `app.state.get_s3_credentials`
(the latter two default to `None`). If both `EARTHDATA_USERNAME` and `EARTHDATA_PASSWORD` are
set, exactly one credential object is created depending on `EARTHDATA_S3_DIRECT_ACCESS`:

- **`s3_access=False`** (default): an `EarthdataTokenProvider(username, password)` instance is
  stored at `app.state.earthdata_token_provider`. It lazily fetches and refreshes a bearer token
  from `https://urs.earthdata.nasa.gov/api/users/find_or_create_token` on demand, caching the
  result until within 5 minutes of expiry (`TOKEN_REFRESH_BUFFER`). Thread-safe; a single instance
  is shared across requests.
- **`s3_access=True`**: a `GetS3Credentials(username, password)` instance is stored at
  `app.state.get_s3_credentials`. It caches one `NasaEarthdataCredentialProvider` (from
  `obstore.auth.earthdata`) per S3-credentials endpoint URL in a plain dict protected by a
  `threading.Lock`. Credential refresh within each provider is handled internally by obstore.

The two paths are mutually exclusive: when S3 direct access is enabled no bearer token provider
is created, and vice versa.

### Per-request propagation

`BackendParams` is a FastAPI dependency that runs on every request. It reads
`app.state.{earthdata_token_provider, s3_access, get_s3_credentials}` and passes them into
`CMRBackend`. If a token provider is present, `BackendParams.__init__` calls it immediately to
resolve the current bearer token string. `CMRBackend.__attrs_post_init__` then merges the token,
`s3_access` flag, and `get_s3_credentials` callable into `reader_options`, so every reader
instance receives them as constructor arguments.

### Per-granule credential provider (`NasaEarthdataCredentialProvider`)

When `s3_access=True`, each reader calls `granule.s3_credentials_endpoint` during
`__attrs_post_init__`. This property scans the granule's `related_urls` for a URL containing
`/s3credentials` and raises `S3CredentialsEndpointMissing` if none is found (different DAACs
expose different URLs). The reader then calls `get_s3_credentials(endpoint)` to retrieve the
cached `NasaEarthdataCredentialProvider` instance for that endpoint.

`NasaEarthdataCredentialProvider` is provided by `obstore.auth.earthdata` and is constructed
with the endpoint URL and `(username, password)` credentials. obstore manages credential
fetching and refresh internally.

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

---

## Lambda Deployment

The Lambda function is built as a zip package using a Docker build environment (not a container
image Lambda). CDK's `Code.from_docker_build` runs the Dockerfile to produce the deployment
artifact. GDAL and PROJ data files are bundled inside the package and their paths are set as
environment variables (`GDAL_DATA`, `PROJ_DATA`) at the top of `handler.py`, before any import
that triggers GDAL or PROJ context creation.

[Mangum](https://github.com/jordaneremieff/mangum) translates Lambda event/context objects into
ASGI scope/receive/send, allowing the FastAPI app to run unmodified. Mangum is configured with
`lifespan="off"` because its lifespan support runs startup/shutdown on every invocation rather
than only during cold starts. Instead, `startup(app)` is called once at module import time so
it runs on cold starts and is then frozen into the SnapStart snapshot.

SnapStart is enabled on published versions. CDK publishes a version automatically and creates a
`live` alias pointing at it; API Gateway integrates with the alias rather than `$LATEST` so the
snapshot is actually used.

---

## Observability (OTEL + X-Ray)

Telemetry is opt-in via `TITILER_CMR_TELEMETRY_ENABLED`. When disabled, no OTEL packages are
imported and the Lambda function has `Tracing.DISABLED`. When enabled, the CDK stack sets
`Tracing.ACTIVE` on the function and grants the execution role the IAM actions required for
the X-Ray native OTLP ingestion endpoint (`xray:PutSpans`, `xray:PutSpansForIndexing`,
`xray:PutTraceSegments`, `xray:PutTelemetryRecords`).

### Instrumentation setup

The entire OTEL setup lives inside an `if "AWS_EXECUTION_ENV" in os.environ` guard in
`handler.py`, so it is skipped in local development and tests.

Three instrumentors are activated:

- `FastAPIInstrumentor` — patches `app.build_middleware_stack` to wrap the ASGI middleware stack
  with `OpenTelemetryMiddleware`, creating a server span for every HTTP request.
- `HTTPXClientInstrumentor` — instruments the httpx `Client` created by `startup()`. It is
  called before `startup()` so the client is captured at construction time.
- `LoggingInstrumentor(set_logging_format=True)` — replaces the log record factory to inject
  `otelTraceID` and `otelSpanID` into every log record emitted while a span is active.
  `set_logging_format=True` is required for injection; despite the name it does not override
  `XRayJsonFormatter` because `logging.basicConfig()` is a no-op when handlers already exist.

### Trace export

Spans are exported synchronously via `SimpleSpanProcessor` (not `BatchSpanProcessor`).
`SimpleSpanProcessor` has no background flush thread, which is correct for Lambda: the export
HTTP call completes before the invocation returns, and there is no thread to lose during a
SnapStart snapshot or freeze.

The exporter sends OTLP/protobuf to X-Ray's native OTLP endpoint
(`https://xray.{region}.amazonaws.com/v1/traces`). Requests are signed with AWS SigV4 using
the function's execution role credentials, resolved at request time via botocore (which is
available in the Lambda runtime and does not need to be bundled). A response hook logs any
non-2xx export responses at ERROR level for diagnosing auth or signing failures.

`AwsXRayIdGenerator` is used so that generated trace IDs encode the epoch timestamp in their
first 8 hex characters — the format X-Ray requires for its time-indexed trace lookup.

### Trace context propagation and sampling

`AwsXRayPropagator` is set as the global text map propagator. It extracts the `X-Amzn-Trace-Id`
header that API Gateway forwards from Lambda's `_X_AMZN_TRACE_ID` environment variable, making
the OTEL trace a continuation of the same trace that Lambda's native X-Ray segment belongs to.

The `TracerProvider` uses the default `ParentBased(root=ALWAYS_ON)` sampler. Because OTEL
inherits the sampling flag from the extracted `X-Amzn-Trace-Id` header, the OTEL sampling
decision is always consistent with Lambda's X-Ray active tracing decision. Invocations sampled
by X-Ray (`Sampled=1`) produce OTEL spans in X-Ray; unsampled invocations (`Sampled=0`) do not.
The X-Ray sampling rate is controlled by X-Ray sampling rules and the `Tracing.ACTIVE` setting
on the function — not by the OTEL SDK.

### Latency and SnapStart tradeoffs

Because sampling is inherited from the `X-Amzn-Trace-Id` header, only invocations where
`Sampled=1` produce spans — and therefore only those invocations pay the cost of the
synchronous OTLP export. Unsampled invocations incur no export overhead.

For sampled invocations, `SimpleSpanProcessor` adds one HTTPS round-trip to
`xray.{region}.amazonaws.com` before the response is returned. The alternative — a Lambda
extension running an OTEL collector sidecar — would replace this with a loopback write
(much cheaper per request) and flush to X-Ray after the handler returns. However, a sidecar
introduces SnapStart complications: the extension's outbound connection to X-Ray is stale
after a restore and must reconnect on the first post-restore invocation, and any spans
buffered in the collector at snapshot time risk being lost or exported twice. Using
`SimpleSpanProcessor` directly in the handler avoids both problems — there is no external
process state to reconcile and no spans in-flight when a snapshot is taken.

### Log correlation

`XRayJsonFormatter` (in `titiler/cmr/logger.py`) includes the active OTEL trace ID in every log
record as `xray_trace_id` in X-Ray format (`1-{epoch8}-{id24}`). When a span is active (i.e.
during request handling), this field is derived from the `otelTraceID` injected by
`LoggingInstrumentor`, which matches the trace ID on the exported span. For log lines emitted
outside an active span (cold start, background tasks), it falls back to `_X_AMZN_TRACE_ID` from
the environment. The `otelTraceID` and `otelSpanID` fields are also emitted as separate fields
on every in-span log record.
