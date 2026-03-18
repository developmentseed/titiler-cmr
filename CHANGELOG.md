# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added

- Restructure API into two separate backends with distinct route prefixes:
  `/xarray` for NetCDF/HDF5 datasets (via xarray) and `/rasterio` for
  multi-band raster files (via rio-tiler/GDAL)
- `orbit_number` query parameter for filtering granules by orbit number
- Multi-variable visualization support in xarray backend
- Legacy route redirects (`titiler/cmr/legacy.py`) for backwards compatibility
  with old API paths and parameter names
- Architecture documentation (`docs/architecture.md`)
- Earthdata Login (EDL) based S3 direct access: bearer token fetched from the
  EDL token endpoint and auto-refreshed before expiry; temporary per-DAAC S3
  credentials cached for 50 minutes; falls back to HTTPS with
  `Authorization: Bearer` when S3 direct access is unavailable
  ([#112](https://github.com/developmentseed/titiler-cmr/pull/112))
- Update deployment application to add EARTHDATA_USERNAME and EARTHDATA_PASSWORD
  to Lambda environment
  ([#120](https://github.com/developmentseed/titiler-cmr/pull/120))
- Multiple additional CMR Granule Search parameters:
  - `skipcovered`: skip granules whose footprint is already fully covered by previously-selected granules
  - `coverage_tolerance`: spatial buffer (degrees) for coverage checks; defaults differ by backend (xarray: 0.0, rasterio: 0.01)
  - `exitwhenfull`: stop fetching granules once the requested area is fully covered (default: true)
  - `f` (on `/granules` endpoints): response format — `json` (default list) or `geojson` (FeatureCollection with footprints)
  - `cloud_cover`: filter granules by cloud cover percentage range
  - `attribute`: filter by additional CMR granule attributes
  - `sort_key`: control CMR result ordering
- Multi-variable xarray support: `variables` parameter (plural) accepts repeated values (e.g., `variables=sst&variables=precip`)
- GeoJSON output format for `/granules` endpoints via `f=geojson`, returning granule spatial footprints as a FeatureCollection
- Automatic expression translation: expressions using named assets or xarray variables (e.g., `B04/B03`, `sst/error`) are automatically rewritten to positional band references (`b1/b2`) required by rio-tiler ≥ 9
- Xarray backend uses [obstore](https://github.com/developmentseed/obstore) and [obspec-utils](https://github.com/developmentseed/obspec-utils) for cloud-native dataset loading, replacing fsspec
- Migration guide (`docs/migration.md`) documenting breaking changes and upgrade paths from pre-1.0 API

### Fixed

- Fix the missing parameter error message "`indexes` must be provided if not
  providing `bands_regex` and `bands`". The
  [alias](https://fastapi.tiangolo.com/tutorial/query-params-str-validations/#alias-parameters)
  for the `indexes` parameter is `bidx`, so the `bidx` parameter is what is
  missing from the parameter string.
  ([#101](https://github.com/developmentseed/titiler-cmr/pull/101))
- Fix rasterio/GDAL environment inheritence when using multithreaded reads
  ([#91](https://github.com/developmentseed/titiler-cmr/issues/91))
- Fix `get_concept_id_umm` function to raise 404, not 400, when there is no
  concept found for the given concept ID
  ([#113](https://github.com/developmentseed/titiler-cmr/issues/113))
- Fix response when `temporal` parameter is not supplied for datetime
  interpolation, such that the response status is 400, not 500
  ([#122](https://github.com/developmentseed/titiler-cmr/issues/122))

### Changed

- Backend selection moved from `?backend=` query parameter to URL prefix
  (`/xarray/` or `/rasterio/`); the old `?backend=` parameter is accepted by
  legacy redirects for backwards compatibility, defaulting to rasterio when absent
- `variable` (singular) renamed to `variables` (plural) for xarray backend;
  old name still accepted via redirect
- S3 authentication is now exclusively via Earthdata Login bearer tokens and
  per-DAAC temporary S3 credentials; AWS IAM / role-based access is no longer used
- Full backwards compatibility with pre-1.0 API: old root-level paths
  automatically redirect (301/308) to the appropriate backend prefix, with
  parameter renames (`concept_id`, `datetime`, `bands_regex`, `variable`)
  applied transparently during redirect
- `concept_id` query parameter renamed to `collection_concept_id` to align
  with CMR naming; old name still accepted via redirect
- `datetime` query parameter renamed to `temporal` to align with CMR standard;
  old name still accepted via redirect
- `bands_regex` query parameter renamed to `assets_regex`; old name still
  accepted via redirect
- CMR granule search now uses `granules.umm_json` format instead of
  `granules.json`
- Timeseries logic extracted from `factory.py` into `titiler/cmr/timeseries.py`
- Simplify function `get_geojson_bounds` and add illustrative examples to its
  docstring ([#115](https://github.com/developmentseed/titiler-cmr/issues/115))
- Upgraded to titiler>=2.0,<3 and rio-tiler>=9.0,<10

## [v0.3.0]

### Added

- interpolate {datetime} in if sel includes {dim}={datetime} ([#78](https://github.com/developmentseed/titiler-cmr/pull/78))
- /compatibility and /concept_metadata endpoints ([#80](https://github.com/developmentseed/titiler-cmr/pull/80))
- add dataset statistics to /compatibility endpoint output for xarray datasets ([#80](https://github.com/developmentseed/titiler-cmr/pull/82))

- export traces to X-Ray from AWS Lambda ([#81](https://github.com/developmentseed/titiler-cmr/pull/81))

### Fixed

- Fix bands + indexes + bands_regex logic ([#89](https://github.com/developmentseed/titiler-cmr/pull/89))
- DRY out the AWS credential logic in backend.py ([#93](https://github.com/developmentseed/titiler-cmr/pull/93))

### Changed

- Switch from a zip package to a containerized Lambda function ([#81](https://github.com/developmentseed/titiler-cmr/pull/81))

## [v0.2.0]

### Fixed

- Upgrade to Python 3.12 runtime to avoid dependency build errors

### Added

- Add OTEL traces
- Upgrade to titiler v0.23. and add more logging statements to help track performance ([#71](https://github.com/developmentseed/titiler-cmr/pull/71))

## [v0.1.5]

### Fixed

- Allow POST requests from cross-origin clients ([#60](https://github.com/developmentseed/titiler-cmr/pull/60))

- Handle case where `ResolutionAndCoordinateSystem` is not available for calculating time series request size ([#62](https://github.com/developmentseed/titiler-cmr/pull/62))

- Pin geojson-pydantic to >=2.0.0,<3.0 (previously was >=1.1.1) and fixed iteration of `FeatureCollection` ([#68](https://github.com/developmentseed/titiler-cmr/pull/68))

### Changed

- Add `s3_auth_strategy` and `aws_request_payer` to `AppSettings`: <https://github.com/developmentseed/titiler-cmr/pull/58>

## [v0.1.4]

### Added

- Added support for `sel` and `sel_method` parameters: <https://github.com/developmentseed/titiler-cmr/pull/52>
- Add HLS tile configuration documentation: <https://github.com/developmentseed/titiler-cmr/pull/45>

### Fixed

### Changed

- Added github workflow for deploying to a dev stack: <https://github.com/developmentseed/titiler-cmr/pull/51>
- Modifications to github workflow to support deployment via [github.com/NASA-IMPACT/veda-deploy](https://github.com/NASA-IMPACT/veda-deploy): <https://github.com/developmentseed/titiler-cmr/pull/47>, <https://github.com/developmentseed/titiler-cmr/pull/48>
- Modifications to deployment environment variables `STAGE` and `AWS_REQUEST_PAYER` via `additional_env` in `StackSettings`: <https://github.com/developmentseed/titiler-cmr/pull/55>

## [v0.1.3]

### Added

- Time series API limits

## [0.1.2]

### Added

- Support for `/timeseries` endpoints ([#33](https://github.com/developmentseed/titiler-cmr/pull/33))

## [0.1.1]

### Added

- Add `/bbox`, `/feature`, and `/statistics` endpoints ([#30](https://github.com/developmentseed/titiler-cmr/pull/30))

## [0.1.1]

### Added

- Ability to run locally with Earthdata authentication ([#28](https://github.com/developmentseed/titiler-cmr/pull/28))

[Unreleased]: <https://github.com/developmentseed/titiler-cmr/compare/v0.3.0..develop>
[v0.3.0]: <https://github.com/developmentseed/titiler-cmr/compare/v0.2.0..v0.3.0>
[v0.2.0]: <https://github.com/developmentseed/titiler-cmr/compare/v0.1.5..v0.2.0>
[v0.1.5]: <https://github.com/developmentseed/titiler-cmr/compare/v0.1.4..v0.1.5>
[v0.1.4]: <https://github.com/developmentseed/titiler-cmr/compare/v0.1.3..v0.1.4>
[v0.1.3]: <https://github.com/developmentseed/titiler-cmr/compare/v0.1.2..0.1.3>
[0.1.2]: <https://github.com/developmentseed/titiler-cmr/compare/0.1.1..0.1.2>
[0.1.1]: <https://github.com/developmentseed/titiler-cmr/compare/0.1.0..0.1.1>
