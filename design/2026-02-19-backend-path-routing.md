# ADR: Backend-Specific URL Path Routing

- **Status:** Proposed
- **Date:** 2026-02-19
- **Deciders:** @abarciauskas-bgse @chuckwondo @hrodmn

---

## Context

titiler-cmr supports two raster backends for serving data from NASA CMR:

- **rasterio** — for Cloud Optimized GeoTIFFs (COGs) and other GDAL-readable formats
- **xarray** — for NetCDF, Zarr, and other array-oriented formats

Supporting two backends with meaningfully different parameter sets within a single tiling API is inherently complex. The current design handles this by accepting a `backend` query parameter that selects the reader at runtime:

```
GET /tiles/{tms}/{z}/{x}/{y}?backend=rasterio&concept_id=C123...
GET /tiles/{tms}/{z}/{x}/{y}?backend=xarray&concept_id=C123&variable=temp...
```

The `backend` parameter is not optional — callers have always been required to specify which backend to use on every request. This change does not shift that responsibility to the user; it simply moves the selection from the query string into the URL path, where it is more visible and can be handled by the routing layer rather than application code.

This was a pragmatic choice that allowed both backends to be shipped quickly under a unified API surface. As the service has matured and usage has grown, there are now clear opportunities to make the API clearer for users and simpler for contributors:

1. **API documentation and parameter discoverability.** Because both backends share the same endpoints, the OpenAPI schema exposes every parameter from both backends on every endpoint. Surfacing xarray-specific options (e.g., `variable`, `sel`) alongside rasterio-specific options on the same endpoint makes it harder for users to understand what is needed for their use case.

2. **Parameter validation.** Cross-field constraints — such as `variable` being required for xarray but irrelevant for rasterio — are enforced in application code. Moving backend selection into the URL path would allow the framework to own this validation and produce more precise error messages.

3. **Internal code complexity.** Routing between backends at request time requires conditional logic spread across the codebase. Separating backends at the routing level would simplify this substantially, reduce the amount of custom code maintained in titiler-cmr, and make it easier for new contributors to understand each backend in isolation.

4. **URL transparency.** Having the backend visible in the URL path makes it straightforward to differentiate traffic in logs, monitoring dashboards, CDN cache keys, and API gateway routing rules.

---

## Decision

We will move backend selection from a query parameter into the URL path:

```
GET /rasterio/tiles/{tms}/{z}/{x}/{y}?concept_id=C123...
GET /xarray/tiles/{tms}/{z}/{x}/{y}?concept_id=C123&variable=temp...
```

Each backend prefix (`/rasterio/...`, `/xarray/...`) will be served by a dedicated set of route handlers that only expose parameters relevant to that backend.

The following endpoints will be available under each prefix:

| Endpoint | Description |
|---|---|
| `/{backend}/tiles/{tms}/{z}/{x}/{y}` | Map tile |
| `/{backend}/{tms}/tilejson.json` | TileJSON metadata |
| `/{backend}/{tms}/map` | HTML map viewer |
| `/{backend}/bbox/{minx},{miny},{maxx},{maxy}.{format}` | Bounding box image |
| `/{backend}/feature` | GeoJSON feature image |
| `/{backend}/statistics` | Statistical summary |
| `/{backend}/timeseries/...` | Time series endpoints |

Backend-agnostic endpoints (landing page, `/conformance`, `/tileMatrixSets`, `/compatibility`) remain at the root level and are unaffected by this change.

---

## Consequences

### Benefits

**Clearer API documentation.** Each backend exposes only the parameters it actually accepts. The `/rasterio/...` endpoints will not show `variable`, `sel`, or xarray dataset options. The `/xarray/...` endpoints will not show rasterio band/index options. Users browsing the API documentation see an accurate and focused picture of what each backend requires, and the Swagger UI can group endpoints by backend for easier navigation.

**Better parameter validation and error messages.** Required and optional parameters for each backend are declared at the route level. If a caller omits the required `variable` parameter on an xarray endpoint, the framework returns a descriptive 422 error listing the missing field — rather than a generic error from application-level validation logic.

**Simpler internals and easier contributor onboarding.** With backend selection moved to the routing layer, the runtime conditional logic that currently routes between backends inside endpoint handlers can be removed. Each backend's code path becomes self-contained, which reduces the overall amount of custom code in titiler-cmr and lowers the learning curve for new contributors who only need to understand one backend at a time. This separation should also allow titiler-cmr to more closely follow and inherit from the upstream titiler factory classes, reducing divergence and maintenance burden.

**Clear URL semantics.** The backend is visible in the URL, enabling:
- Log analysis and dashboarding by backend without query string parsing
- Cache key differentiation at the CDN or proxy layer
- Simpler routing rules in API gateways or load balancers

**Extensibility.** Adding a third backend in the future requires adding a new path prefix with its own parameter set. No shared endpoint logic needs to change.

### Trade-offs

**Breaking URL change.** Any existing client that constructs URLs with `?backend=...` will need to be updated. This is mitigated by the deprecation strategy described below.

**Increased route count.** The total number of registered routes roughly doubles. This has no meaningful runtime performance impact but increases the surface area of the OpenAPI schema.

---

## Alternatives Considered

### Keep query parameter

We could keep the query parameter and create a more complex parameter validation system and refactor the `Endpoints` and `CMRBackend` classes to be more clear. This would result in a more complex codebase than with the path-based backend routing and would not solve the problem of the unclear query parameter combination constraints.


---

## Rollout Plan

### Minor release (v0.4.0 or v0.5.0)

- Introduce `/rasterio/...` and `/xarray/...` path-prefixed endpoints alongside the existing endpoints.
- Existing endpoints (`/tiles/...?backend=...`) continue to function and return deprecation warnings in the response headers (`Deprecation: true`, `Link: <new-url>; rel="successor-version"`).
- Documentation is updated to use the new URL structure. The migration guide (see below) is published.
- No action required from existing users in this release.

### Major release (v1.0.0)

- Legacy `?backend=...` endpoints are removed.
- The `backend` query parameter is no longer accepted on any endpoint.
- Callers that have not migrated will receive `404` responses.

---

## Migration Guide

Replace `?backend=<value>` query parameter usage with the corresponding path prefix and remove the `backend` key from the query string.

**Before:**
```
/tiles/WebMercatorQuad/5/8/12?backend=xarray&concept_id=C123&variable=analysed_sst
/WebMercatorQuad/tilejson.json?backend=rasterio&concept_id=C456&bidx=1
```

**After:**
```
/xarray/tiles/WebMercatorQuad/5/8/12?concept_id=C123&variable=analysed_sst
/rasterio/WebMercatorQuad/tilejson.json?concept_id=C456&bidx=1
```

The `concept_id` and all other parameters are unchanged. Only the backend selection and the URL structure differ.
