---
title: refactor: Update compatibility assessment reporting
type: refactor
status: completed
date: 2026-07-01
---

# refactor: Update compatibility assessment reporting

## Summary

Update the compatibility report pipeline to consume `docs/compatibility/assessment-results-2026-07-01.parquet`, understand its expanded failure schema, and produce trustworthy summaries for the new assessment methodology. The work should preserve the searchable table/report workflow while replacing stale 2025-era reason buckets with categories grounded in `failure_stage`, `failure_category`, `failure_subcategory`, `error_code`, and selected `raw_error_body` patterns.

---

## Problem Frame

The new assessment file differs from the prior report inputs and reflects a more realistic authentication methodology. Some collections that previously appeared compatible are now incompatible because the assessment no longer bypasses access or credential failures in a non-standard way. The current report code and notebook still read `tiling_results-11-24-2025.parquet` and discuss old incompatibility reasons such as `unsupported_format`, `no_xy_dimensions`, and `group_structure`, so the generated report would mislead readers if run against the new file without updating the categorization model.

---

## Requirements

- R1. Read and summarize `docs/compatibility/assessment-results-2026-07-01.parquet` as the current source of truth.
- R2. Document and use the new schema columns that describe failure stage, category, subcategory, error code, HTTP status, endpoint, and raw error body.
- R3. Produce human-facing incompatibility categories that distinguish unsupported assets, data incompatibilities, metadata gaps, inaccessible assets/authentication, service errors, render errors, and inconclusive sampling/bbox cases.
- R4. Inspect `raw_error_body` where the structured fields are too coarse, especially for compatibility HTTP 500 responses and access/credential failures.
- R5. Update both the narrative report and searchable table so the report can be regenerated from the 2026-07-01 file.
- R6. Add focused tests or validation fixtures around the categorization logic so future assessment files can be reviewed without hand-editing fragile notebook code.

---

## Scope Boundaries

- Do not change the assessment methodology or re-run the assessment in this plan.
- Do not change TiTiler-CMR runtime compatibility endpoint behavior unless reporting reveals a separate product bug worth filing.
- Do not treat service errors as compatible just because they may be transient. Report them separately from proven data incompatibility.
- Do not include generated `build/` artifacts in implementation review; regenerate docs only as verification output if needed.

### Deferred to Follow-Up Work

- Endpoint fixes for recurring server-side errors such as `{"detail":"'NoneType' object has no attribute 'get'"}` should be filed or planned separately after the report classifies their impact.
- Assessment reruns to validate any endpoint fixes are outside this reporting update.

---

## Context & Research

### Relevant Code and Patterns

- `docs/compatibility/generate_table.py` currently hard-codes `tiling_results-11-24-2025.parquet`, selects legacy display columns, writes `docs/compatibility/results_table.html`, and uses `print` rather than logging.
- `docs/compatibility/index.ipynb` and `docs/compatibility/report.ipynb` currently read `tiling_results-11-24-2025.parquet` and contain narrative sections for legacy reasons: unsupported formats, no X/Y dimensions, group structure, and other issues.
- `docs/compatibility/plotting.py` provides pie chart helpers used by the notebooks.
- `pyproject.toml` already includes `pandas`, `pyarrow`, `jupyter`, `matplotlib`, and docs tooling in the dev dependency group, so no new dependency should be needed.

### New Assessment Schema Snapshot

`docs/compatibility/assessment-results-2026-07-01.parquet` has 9,665 rows and 49 columns. Important new/current columns include:

- Identity and metadata: `collection_concept_id`, `collection_short_name_and_version`, `concept_id`, `data_center`, `data_url`, `backend`, `format`, `extension`, `num_granules`, `processing_level`, `groups`.
- Compatibility result: `tiling_compatible`, `incompatible_reason`, `error_message`, `tiles_url`, `variable`, `data_variables`.
- Structured failure classification: `assessment_status`, `failure_stage`, `failure_category`, `failure_subcategory`, `error_code`, `error_detail`, `failure_http_status_code`, `failure_endpoint`, `failure_url`, `raw_error_body`.
- Probe details: `granule_concept_id`, `granule_ur`, `assessed_asset_href`, `assessed_asset_extension`, `assessed_asset_scheme`, `group`, `variables`, `selected_variable_source`, `compatible_groups`, `tested_groups`, `granule_bbox`, `probe_bbox`, `bbox_status_code`, `bbox_error_body`, `bbox_error_snippet`, `bbox_url`, `bbox_attempt_count`, `bbox_attempts`, `bbox_probe_limited`, `compatibility_status_code`, `compatibility_error_body`, `compatibility_response`.

Initial counts from the new file:

- `tiling_compatible`: 719 true, 8,946 false.
- `assessment_status`: 719 compatible, 5,633 incompatible, 3,313 inconclusive.
- Top `failure_category` values: `data_incompatible` 3,481; `unsupported_asset` 2,152; `metadata_incomplete` 1,789; `service_error` 1,176; `inaccessible` 293; `render_error` 50; `request_error` 5.
- Top `error_code` values: `unsupported_media_type` 2,152; `no_tileable_variables` 1,518; `missing_xy_spatial_coordinates` 1,430; `missing_asset_href_or_extension` 1,342; `compatibility_http_500` 1,120; `no_granule_found` 332; `s3_credential_lookup_failed` 293; `unsupported_dimensionality` 293; `unsupported_file_signature` 158; `invalid_or_missing_granule_bbox` 115.

### Institutional Learnings

- No existing `dev-docs/solutions/` learnings were present for this reporting flow.

### External References

- External research is not needed for this update. The work is driven by the local parquet schema, local report code, and project docs build conventions.

---

## Key Technical Decisions

- Centralize categorization in a Python module rather than embedding all logic in notebooks: this makes raw-error-body classification testable and keeps the report narrative reproducible.
- Treat `assessment_status == "inconclusive"` as a first-class outcome in summaries, not as simply incompatible. It can still appear under non-compatible totals, but the report should explain that no compatibility conclusion was reached for those rows.
- Prefer structured fields over free-text parsing. Use `raw_error_body` only to refine broad buckets such as `compatibility_http_500`, `bbox_http_500`, `decode_error`, and credential/access errors.
- Keep source-data columns in the searchable table but add human-readable category columns, so readers can filter at both the raw and summarized levels.
- Preserve generated docs outputs as build artifacts, but make the source notebook/scripts deterministic enough that reviewers can regenerate them locally.

---

## Open Questions

### Resolved During Planning

- Should authentication-related failures be hidden to keep compatibility numbers comparable with prior reports? No. The new methodology intentionally surfaces standard access behavior, so authentication/credential failures should be reported as incompatible or inconclusive access outcomes rather than filtered away.
- Should service errors be merged into data incompatibility? No. HTTP 500/503 failures are operational/reporting categories and should be separated from proven unsupported data structure.

### Deferred to Implementation

- Exact names for the normalized report columns: choose names while implementing, likely something like `report_status`, `report_category`, `report_reason`, and `report_reason_detail`.
- Whether `compatibility_http_500` can be split into more specific subcategories solely from `raw_error_body`: inspect all distinct bodies and add only high-confidence rules.
- Whether both `index.ipynb` and `report.ipynb` are still needed: update both if they remain published or consolidate only after confirming docs navigation.

---

## Implementation Units

### U1. Add testable compatibility report categorization

**Goal:** Create a small reporting module that loads assessment rows and derives stable human-facing status/category/reason fields from the new schema.

**Requirements:** R1, R2, R3, R4, R6

**Dependencies:** None

**Files:**
- Create: `docs/compatibility/reporting.py`
- Create: `tests/test_compatibility_reporting.py`

**Approach:**
- Add public functions with docstrings for loading the parquet file and classifying rows or dataframes.
- Base classification primarily on `assessment_status`, `failure_category`, `failure_subcategory`, and `error_code`.
- Add narrow raw-body detectors for high-confidence patterns already visible in the new file, including S3 credential lookup/IMDS failures, unsupported media type, missing asset href/extension, missing X/Y coordinates, unsupported dimensionality, decode errors, and generic service unavailable/internal server errors.
- Return normalized columns that notebooks and `generate_table.py` can reuse.

**Execution note:** Implement characterization tests first from representative rows observed in the new assessment counts.

**Patterns to follow:**
- `docs/compatibility/generate_table.py` for current docs-local script placement.
- Project typing preference from `pyproject.toml` and shared instructions: use modern annotations and docstrings for public functions.

**Test scenarios:**
- Happy path: a compatible row with `tiling_compatible=True` and no failure fields classifies as compatible.
- Happy path: `unsupported_media_type` classifies under unsupported asset with a readable unsupported media reason.
- Happy path: `s3_credential_lookup_failed` with an IMDS token error in `raw_error_body` classifies as inaccessible/authentication or credential failure.
- Happy path: `missing_xy_spatial_coordinates` and `missing_y_coordinate` classify as data incompatible/spatial coordinate issues.
- Happy path: `no_granule_found` and `invalid_or_missing_granule_bbox` classify as metadata incomplete or inconclusive metadata gaps.
- Error path: an unknown `compatibility_http_500` body remains service error/HTTP 500 rather than being misclassified as data incompatible.
- Edge case: null/NaN fields do not crash classification and produce an explicit unknown/inconclusive fallback.

**Verification:**
- Categorization tests pass without network access.
- Running classification over `docs/compatibility/assessment-results-2026-07-01.parquet` produces no missing normalized category for any row.

---

### U2. Update searchable table generation for the new file and categories

**Goal:** Make `docs/compatibility/generate_table.py` generate `results_table.html` from the 2026-07-01 assessment and expose both raw and normalized failure fields.

**Requirements:** R1, R2, R3, R5

**Dependencies:** U1

**Files:**
- Modify: `docs/compatibility/generate_table.py`
- Modify: `docs/compatibility/results_table.html` only if regenerated as part of verification
- Test: `tests/test_compatibility_reporting.py`

**Approach:**
- Replace the hard-coded old parquet filename with the current file path, ideally as a default argument or constant that can be overridden in tests.
- Import the categorization helper from `docs/compatibility/reporting.py`.
- Include display columns such as collection, data center, backend, format, extension, `assessment_status`, normalized report category/reason, raw `error_code`, `failure_stage`, `failure_http_status_code`, processing level, and granule count.
- Add table filters for normalized category/reason, assessment status, data center, backend, and compatibility.
- Replace `print` with stdlib `logging`.

**Test scenarios:**
- Happy path: a tiny dataframe with new-schema columns renders records containing normalized category fields.
- Edge case: optional display columns missing from a fixture are skipped without failure, matching the current script's defensive pattern.
- Integration: generated table metadata counts distinguish compatible, incompatible, and inconclusive rows.

**Verification:**
- `uv run python docs/compatibility/generate_table.py` succeeds from `docs/compatibility/` or the repository root, depending on the script's documented invocation.
- The generated table can filter by assessment status and normalized reason.

---

### U3. Refresh notebook report narrative and charts

**Goal:** Update the compatibility report notebooks so the published report explains the new methodology, schema, compatibility rate, and current incompatibility buckets.

**Requirements:** R1, R2, R3, R4, R5

**Dependencies:** U1

**Files:**
- Modify: `docs/compatibility/index.ipynb`
- Modify: `docs/compatibility/report.ipynb`
- Modify: `docs/compatibility/plotting.py` only if chart helper changes are needed

**Approach:**
- Update parquet reads to use `assessment-results-2026-07-01.parquet`.
- Add classification helper import and derive normalized report columns before charting.
- Replace legacy narrative sections (`unsupported_format`, `no_xy_dimensions`, `group_structure`) with sections that match current categories: compatible summary, assessment-status breakdown, unsupported assets, data incompatibilities, metadata incomplete/inconclusive cases, inaccessible/authentication failures, service/render/request errors, and raw-error-body-derived subcategories.
- Explicitly call out that compatibility counts are not directly comparable to the old report because authentication is now assessed through the standard path.
- Update charts/tables to group by normalized category/reason rather than `incompatible_reason` alone.

**Test scenarios:**
- Test expectation: none -- notebooks are documentation artifacts, but the logic they use should live in `docs/compatibility/reporting.py` and be covered by U1 tests.

**Verification:**
- The notebooks execute through the docs build or a local notebook execution command without stale column-name errors.
- The rendered report no longer references old-only reason buckets unless discussing historical methodology differences.

---

### U4. Add schema and regression checks for future assessment updates

**Goal:** Make future compatibility report updates easier by adding explicit schema validation/reporting around required and optional columns.

**Requirements:** R2, R6

**Dependencies:** U1

**Files:**
- Modify: `docs/compatibility/reporting.py`
- Modify: `tests/test_compatibility_reporting.py`

**Approach:**
- Define required columns for minimal report generation and optional columns for richer drill-downs.
- Provide a helper that returns or logs missing required/optional columns before report generation.
- Keep validation lightweight: fail for missing required columns, warn or degrade gracefully for optional columns.

**Test scenarios:**
- Error path: missing `tiling_compatible` or `assessment_status` raises a clear validation error.
- Edge case: missing optional probe columns still allows categorization and table generation.
- Happy path: the full 2026-07-01 schema validates cleanly.

**Verification:**
- Tests cover schema validation without loading the large parquet file unless a small fixture is created.
- Running validation on the real parquet reports zero missing required columns.

---

## System-Wide Impact

- **Documentation:** The compatibility report and searchable table are the primary affected surfaces.
- **Public interpretation:** Compatibility rates may drop relative to the old report because standard authentication/access behavior is now represented. The report must make this clear to avoid implying a product regression without context.
- **Runtime code:** No TiTiler-CMR application runtime behavior should change in this plan.
- **Generated artifacts:** `build/` outputs should not be used as source context and do not need to be committed unless the project convention explicitly requires generated docs artifacts.
- **Future maintenance:** A centralized categorization module reduces notebook drift and makes future parquet schema changes easier to detect.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Raw-error-body parsing overfits one run's exact text. | Use structured fields first and only add narrow, high-confidence text rules with tests. |
| Inconclusive rows are accidentally counted as proven incompatible. | Keep `assessment_status` visible and include separate compatible/incompatible/inconclusive totals. |
| Service errors obscure actual data incompatibility counts. | Report service/render/request errors as separate operational categories. |
| Notebook code and table script diverge. | Share classification and schema helpers from `docs/compatibility/reporting.py`. |
| New docs code imports from a non-package directory awkwardly. | Keep imports simple for the docs execution context and add tests that import the module the same way implementation expects. |

---

## Documentation Plan

- Update `docs/compatibility/index.ipynb` and `docs/compatibility/report.ipynb` narrative prose.
- Regenerate `docs/compatibility/results_table.html` after the table script is updated.
- Check `mkdocs.yml` only if the published report path or notebook selection changes.

---

## Verification Plan

- Run the targeted reporting tests with `uv run pytest tests/test_compatibility_reporting.py`.
- Run lint/format checks relevant to changed Python files, and pre-commit if configured for this repo.
- Execute the table generation script against `docs/compatibility/assessment-results-2026-07-01.parquet`.
- Execute or build the compatibility notebooks/docs enough to catch stale column references and import path issues.
