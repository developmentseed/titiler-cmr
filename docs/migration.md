# v0.x → v1.0 Migration Guide

titiler-cmr v1.0 introduces substantial breaking changes to the API. The most significant
change is splitting all tiling routes into two separate backends (`/xarray` and `/rasterio`)
with distinct URL prefixes. Several query parameters have also been renamed to align with
CMR conventions.

**Legacy redirects are in place**: old-style URLs are automatically redirected (301/308) to
the new canonical paths, so existing integrations will continue to work during the
transition. However, consumers should update to the new paths to avoid the extra
round-trip overhead.

---

## 1. Endpoint paths now require a backend prefix

All tile, image, and statistics routes now require either `/xarray/` or `/rasterio/` as a
URL prefix. Choose the prefix that matches the data access method your collection uses.

| Old path (pre-1.0) | New rasterio path | New xarray path |
|---|---|---|
| `/tiles/{tms}/{z}/{x}/{y}` | `/rasterio/tiles/{tms}/{z}/{x}/{y}` | `/xarray/tiles/{tms}/{z}/{x}/{y}` |
| `/{tms}/tilejson.json` | `/rasterio/{tms}/tilejson.json` | `/xarray/{tms}/tilejson.json` |
| `/{tms}/map.html` | `/rasterio/{tms}/map.html` | `/xarray/{tms}/map.html` |
| `/preview` | `/rasterio/preview` | `/xarray/preview` |
| `/bbox/{bbox}.{fmt}` | `/rasterio/bbox/{bbox}.{fmt}` | `/xarray/bbox/{bbox}.{fmt}` |
| `/statistics` (POST) | `/rasterio/statistics` | `/xarray/statistics` |
| `/part` (POST) | `/rasterio/part` | `/xarray/part` |
| `/feature` (POST) | `/rasterio/feature` | `/xarray/feature` |
| `/timeseries` | `/rasterio/timeseries` | `/xarray/timeseries` |
| `/timeseries/statistics` (POST) | `/rasterio/timeseries/statistics` | `/xarray/timeseries/statistics` |
| `/timeseries/{tms}/tilejson.json` | `/rasterio/timeseries/{tms}/tilejson.json` | `/xarray/timeseries/{tms}/tilejson.json` |

> **Note:** The `/bbox/{bbox}/assets`, `/point/{lon},{lat}/assets`, and
> `/tiles/{tms}/{z}/{x}/{y}/assets` endpoints are backend-agnostic and **keep the same
> path** — no prefix is needed for these routes.

**Before:**

```
GET /tiles/WebMercatorQuad/5/8/12?collection_concept_id=C2036882064-POCLOUD&temporal=2020-01-01T00:00:00Z/2020-01-02T00:00:00Z
```

**After (rasterio):**

```
GET /rasterio/tiles/WebMercatorQuad/5/8/12?collection_concept_id=C2036882064-POCLOUD&temporal=2020-01-01T00:00:00Z/2020-01-02T00:00:00Z
```

**After (xarray):**

```
GET /xarray/tiles/WebMercatorQuad/5/8/12?collection_concept_id=C2036882064-POCLOUD&temporal=2020-01-01T00:00:00Z/2020-01-02T00:00:00Z
```

---

## 2. `concept_id` renamed to `collection_concept_id`

The query parameter identifying a CMR collection has been renamed to match CMR's own
terminology.

**Before:**

```
GET /tiles/WebMercatorQuad/5/8/12?concept_id=C2036882064-POCLOUD
```

**After:**

```
GET /rasterio/tiles/WebMercatorQuad/5/8/12?collection_concept_id=C2036882064-POCLOUD
```

---

## 3. `datetime` renamed to `temporal`

The time range parameter has been renamed from `datetime` to `temporal` to align with
CMR query conventions.

**Before:**

```
GET /tiles/WebMercatorQuad/5/8/12?concept_id=C2036882064-POCLOUD&datetime=2020-01-01T00:00:00Z/2020-01-02T00:00:00Z
```

**After:**

```
GET /xarray/tiles/WebMercatorQuad/5/8/12?collection_concept_id=C2036882064-POCLOUD&temporal=2020-01-01T00:00:00Z/2020-01-02T00:00:00Z
```

> **Note:** The `{datetime}` interpolation placeholder used inside `sel` parameter values
> is **unchanged**. For example, `sel=time={datetime}` still works as before.

---

## 4. `bands_regex` renamed to `assets_regex`

For rasterio backend requests, the parameter used to filter granule assets by a regular
expression has been renamed.

**Before:**

```
GET /tiles/WebMercatorQuad/5/8/12?concept_id=C2021957657-LPCLOUD&bands_regex=^.*B04.*$
```

**After:**

```
GET /rasterio/tiles/WebMercatorQuad/5/8/12?collection_concept_id=C2021957657-LPCLOUD&assets_regex=^.*B04.*$
```

---

## 5. `variable` renamed to `variables` (xarray backend)

For xarray backend requests, the parameter selecting which dataset variable to render has
been renamed and now accepts multiple values.

**Before:**

```
GET /tiles/WebMercatorQuad/5/8/12?concept_id=C2036882064-POCLOUD&variable=analysed_sst
```

**After:**

```
GET /xarray/tiles/WebMercatorQuad/5/8/12?collection_concept_id=C2036882064-POCLOUD&variables=analysed_sst
```

To select multiple variables, repeat the parameter:

```
GET /xarray/tiles/WebMercatorQuad/5/8/12?collection_concept_id=C2036882064-POCLOUD&variables=analysed_sst&variables=analysis_error
```

---

## 6. New parameters available

The following parameters are new in v1.0. They are not breaking changes but are worth
knowing.

- **`orbit_number`** — Filter CMR granule search results to a specific orbital pass number.
- **`skipcovered`** — Skip granules whose spatial footprint is already fully covered by
  previously selected granules. Reduces redundant data fetching for full-coverage queries.
- **`coverage_tolerance`** — Controls the tolerance used when evaluating coverage for
  `skipcovered`. Accepts a float between 0 and 1.
- **`f`** — On `/assets` endpoints, controls the response format: `json` returns a plain
  list of asset URLs, `geojson` returns a GeoJSON FeatureCollection with granule footprints.

---

## 7. Legacy redirects (backwards compatibility)

Old-style URLs without a backend prefix are automatically redirected to the new canonical
paths:

- **GET** requests receive a **301 Moved Permanently** redirect.
- **POST** requests receive a **308 Permanent Redirect** (method and request body are
  preserved).

When no `?backend=` query parameter is present, the redirect defaults to the **rasterio**
backend. To route a legacy URL to the xarray backend during the transition period, add
`?backend=xarray`:

**Before (legacy, routes to rasterio by default):**

```
GET /tiles/WebMercatorQuad/5/8/12?concept_id=C2036882064-POCLOUD
# → 301 /rasterio/tiles/WebMercatorQuad/5/8/12?collection_concept_id=C2036882064-POCLOUD
```

**Legacy URL routed to xarray:**

```
GET /tiles/WebMercatorQuad/5/8/12?concept_id=C2036882064-POCLOUD&backend=xarray
# → 301 /xarray/tiles/WebMercatorQuad/5/8/12?collection_concept_id=C2036882064-POCLOUD
```

Once you have migrated to the new path prefixes, remove the `?backend=` parameter — it
is only meaningful on the legacy redirect routes and has no effect on the new `/xarray/`
or `/rasterio/` paths.

---

## Migration checklist

- [ ] Update all tile/tilejson/map/preview/bbox/statistics/part/feature URLs to include
      `/xarray/` or `/rasterio/` prefix
- [ ] Replace `concept_id=` with `collection_concept_id=`
- [ ] Replace `datetime=` with `temporal=`
- [ ] Replace `bands_regex=` with `assets_regex=` (rasterio users)
- [ ] Replace `variable=` with `variables=` (xarray users)
- [ ] Remove `?backend=` query parameter (no longer needed after path migration)
- [ ] Verify timeseries URLs use the new `/xarray/timeseries` or `/rasterio/timeseries`
      prefix
