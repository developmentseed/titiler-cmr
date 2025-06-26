# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Fixed

- Allow POST requests from cross-origin clients ([#60](https://github.com/developmentseed/titiler-cmr/pull/60))

- Handle case where `ResolutionAndCoordinateSystem` is not available for calculating time series request size ([#62](https://github.com/developmentseed/titiler-cmr/pull/62))

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

[Unreleased]: <https://github.com/developmentseed/titiler-cmr/compare/0.1.3..develop>
[v0.1.4]: <https://github.com/developmentseed/titiler-cmr/compare/v0.1.4..v0.1.3>
[v0.1.3]: <https://github.com/developmentseed/titiler-cmr/compare/v0.1.3..0.1.2>
[0.1.2]: <https://github.com/developmentseed/titiler-cmr/compare/0.1.2..0.1.1>
[0.1.1]: <https://github.com/developmentseed/titiler-cmr/compare/0.1.1..0.1.0>
