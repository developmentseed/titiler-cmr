# v0.x → v1.0 Migration Guide

## Breaking changes

1. **All routes now require a backend prefix.** Add `/rasterio/` or `/xarray/` before every
   tile, image, statistics, and timeseries path. Choose the prefix that matches the data
   access method your collection uses. This was previously handled with the `backend` query
   parameter.

   > The `/timeseries`, `/bbox/{bbox}/granules`, `/point/{lon},{lat}/granules`, and
   > `/tiles/{tms}/{z}/{x}/{y}/granules` endpoints are backend-agnostic and keep the same
   > path — no prefix needed.

2. **`concept_id` renamed to `collection_concept_id`.** The query parameter identifying a
   CMR collection has been renamed to match CMR's own terminology.

3. **`datetime` renamed to `temporal`.** The time range parameter has been renamed to align
   with CMR query conventions. Note: the `{datetime}` interpolation placeholder used inside
   `sel` parameter values is **unchanged** — `sel=time={datetime}` still works as before.

4. **`bands` renamed to `assets`.** The query parameter for specifying which asset files
   to use for rendering the output image is aligned with the rest of the TiTiler ecosystem.

5. **`bands_regex` renamed to `assets_regex`** (rasterio backend). The parameter used to
   filter granule assets by a regular expression has been renamed.

6. **`variable` renamed to `variables`** (xarray backend). The parameter selecting which
   dataset variable to render has been renamed and now accepts multiple values by repeating
   the parameter: `variables=analysed_sst&variables=analysis_error`.

## Example

**Before:**

```
GET /tiles/WebMercatorQuad/5/8/12?concept_id=C2021957657-LPCLOUD&datetime=2020-01-01T00:00:00Z/2020-01-02T00:00:00Z&backend=rasterio&bands=B04&bands=B03&bands=B02&bands_regex=B[0-9][0-9]
GET /tiles/WebMercatorQuad/5/8/12?concept_id=C1996881146-POCLOUD&datetime=2020-01-01T00:00:00Z/2020-01-02T00:00:00Z&backend=xarray&variable=analysed_sst
```

**After:**

```
GET /rasterio/tiles/WebMercatorQuad/5/8/12?collection_concept_id=C2021957657-LPCLOUD&temporal=2020-01-01T00:00:00Z/2020-01-02T00:00:00Z&assets=B04&assets=B03&assets=B02&assets_regex=B[0-9][0-9]
GET /xarray/tiles/WebMercatorQuad/5/8/12?collection_concept_id=C1996881146-POCLOUD&temporal=2020-01-01T00:00:00Z/2020-01-02T00:00:00Z&variables=analysed_sst
```

## Legacy redirects

Old-style URLs are automatically redirected (301 for GET, 308 for POST) to the new
canonical paths, so existing integrations will continue to work during the transition.
The redirects apply all parameter renames transparently.

```
GET /tiles/WebMercatorQuad/5/8/12?concept_id=C2021957657-LPCLOUD&backend=rasterio
# → 301 /rasterio/tiles/WebMercatorQuad/5/8/12?collection_concept_id=C2021957657-LPCLOUD
```

Update to the new paths when possible to avoid the extra round-trip. Once migrated,
remove the `?backend=` parameter — it has no effect on the new `/rasterio/` or `/xarray/`
paths.

## New parameters
- **`sort_key`**: Sort granules on a field (e.g. `cloud_cover`, `start_date`)
- **`granule_ur`**: Select a specific granule using its `granule_ur`
- **`cloud_cover`**: Filter granule search results to a range of cloud cover values
- **`attribute`**: Filter granule search results using advanced attribute filters
- **`orbit_number`**: Filter granule search results to a specific orbital pass number.
- **`skipcovered`**: Skip granules whose spatial footprint is already fully covered by
  previously selected granules. Reduces redundant data fetching for full-coverage queries.
- **`coverage_tolerance`**: Tolerance for `skipcovered` coverage evaluation (float 0–1).

## New routes
- `/granules` endpoints: returns granule search results. The `f` parameter controls response format: `json` returns a plain list
  of asset URLs, `geojson` returns a GeoJSON FeatureCollection with granule footprints.
