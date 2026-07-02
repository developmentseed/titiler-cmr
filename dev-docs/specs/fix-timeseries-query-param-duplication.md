# Spec: Fix Query-Parameter Duplication in Timeseries Sub-Request URLs

## Context

`/timeseries/*` endpoints accept the same granule-search parameters as their lower-level counterparts (e.g. `collection_concept_id`, `sort_key`, `bounding_box`).  Internally they split the temporal range into discrete steps and then build a list of HTTP GET / POST requests to the underlying endpoint.  The query string of the original timeseries request is supposed to be "passed through" unchanged to those sub-requests.

The function responsible for building those sub-request URLs is `build_request_urls` in `titiler/cmr/timeseries.py`.

## Problem

When a caller passes a parameter such as `sort_key` in the timeseries request URL, that parameter currently appears **twice** in each generated sub-request URL:

1. Once from the raw `request.query_params` (the pass-through copy).
2. Once from the `GranuleSearch` model's `model_dump()` (because `timeseries_cmr_query` copies the parsed value into every per-step `GranuleSearch` object).

CMR rejects duplicate `sort_key` values with a `400 Bad Request`, breaking timeseries calls that use sorting.

This same duplication affects **all** `GranuleSearch` fields that are also present in the original query string, e.g. `collection_concept_id`, `bounding_box`, `cloud_cover`, etc.  The existing test `test_build_request_urls_no_duplicate_params` already fails on `collection_concept_id` duplication.

A secondary serialization bug compounds the issue: `build_request_urls` passes every `model_dump()` value through `str()`, so a Python list like `["cloud_cover"]` becomes the string literal `"['cloud_cover']"` rather than being expanded into repeated `sort_key=cloud_cover` pairs.

## Goals

1. **Primary:** Ensure that each query parameter appears **exactly once** in generated sub-request URLs, even when the same key is present in both the original request and the derived `GranuleSearch` list.
2. **Secondary:** Correctly serialize list-valued fields (e.g. `sort_key`, `attribute`) as repeated `key=value` query parameters, not Python list literals.
3. **Non-goal:** Change the public URL schema or the set of parameters that timeseries endpoints accept.

## Constraints & Assumptions

- `titiler-cmr` uses FastAPI + Pydantic v2.
- `GranuleSearch` is the single source of truth for CMR granule-search parameters. New fields added to `GranuleSearch` should be automatically handled without changing `build_request_urls`.
- Query parameters unrelated to granule search (e.g. `max_size`, `assets`, `expression`) must continue to pass through from the timeseries request to sub-requests, **preserving multiple values for the same key** (e.g. `assets=B04&assets=B05`).
- Legacy aliases such as `concept_id` (alias for `collection_concept_id`) and `datetime` (alias for `temporal`) are parsed by dependency injection.  The raw query string may still contain the alias names, and removing only the canonical field name will leave the alias in the URL.  This is acceptable because the lower-level endpoints also understand those aliases.

- The `temporal` parameter is special: it is part of `TimeseriesParams` so it is already stripped from pass-through params by `timeseries_field_names`.  We do not need to duplicate that logic.

## Architecture Overview

```
build_request_urls
├── derive exclusion set
│   ├── timeseries_field_names       (temporal, step, temporal_mode, datetime)
│   └── granule_search_field_names   (GranuleSearch.model_fields.keys())
├── for each original query param
│   └── keep if NOT in either exclusion set
├── for each GranuleSearch in param_list
│   └── serialize model_dump(exclude_none=True)
│       ├── scalar  -> (key, value)
│       ├── list    -> [(key, v) for v in value]
│       └── None    -> omitted
└── urlencode(combined, doseq=True)
```

The fix is localized entirely to `build_request_urls` and its test.

## API / Interface Design

No new public API.  The signature of `build_request_urls` remains unchanged:

```python
def build_request_urls(
    base_url: str,
    request: Request,
    param_list: List[BaseModel],
) -> List[str]:
    ...
```

## Data Model

A module-level set is added for efficient membership testing:

```python
granule_search_field_names = set(GranuleSearch.model_fields.keys())
```

This set contains:

- `collection_concept_id`
- `concept_id` — *not included* (it is a `validation_alias`, not a field name)
- `granule_ur`
- `temporal`
- `cloud_cover`
- `bounding_box`
- `sort_key`
- `orbit_number`
- `attribute`

(see note on aliases in Constraints)

## Integration Points

- **Caller:** `TimeseriesExtension.register_statistics`, `register_tilejson`, and `register_images` all call `build_request_urls`.  They are unaffected.
- **CMR query generation:** `timeseries_cmr_query` returns `List[GranuleSearch]`.  That code does not need to change.
- **Tests:** `tests/test_timeseries.py::test_build_request_urls_no_duplicate_params` needs to be extended to cover the list-serialization and alias edge cases.

## Migration Path

No migration needed.  This is a bug fix with no external API changes.

## Testing Strategy

1. **Fix the existing failing test:** `test_build_request_urls_no_duplicate_params` already fails because `collection_concept_id` appears twice.  The fix should make it pass.
2. **Expand the test to cover `sort_key` duplication:**  Construct a request that includes `sort_key=cloud_cover` in the query string AND in the `GranuleSearch` model, and assert that only the model value appears (exactly once).
3. **Verify list expansion:**  A `GranuleSearch` with `sort_key=["-start_date", "granule_ur"]` must produce `sort_key=-start_date&sort_key=granule_ur` in the URL.
4. **Verify pass-through preservation:**  A non-GranuleSearch param such as `max_size=512` must still appear in the output URL.
5. **Verify temporal stripping:**  `temporal` and `step` must be removed entirely even when they appear in the original query string.
6. **Verify multi-value pass-through preservation:**  Params such as `assets=B04&assets=B05` must survive exactly as multiple `assets` keys, not be flattened into a single value or a stringified Python list.

### Example test scenario — multi-value pass-through

```python
scope = {
    "type": "http",
    "method": "GET",
    "path": "/rasterio/timeseries/bbox/-100,40,-90,50.png",
    # assets is repeated — lower-level endpoints expect multiple values
    "query_string": b"collection_concept_id=C123&assets=B04&assets=B05&max_size=512&step=P1D",
    "headers": [],
}
request = Request(scope)
param_list = [
    GranuleSearch(
        collection_concept_id="C123",
        temporal="2024-01-01T00:00:00Z/2024-01-01T23:59:59Z",
    ),
]

urls = build_request_urls("http://testserver/bbox/...png", request, param_list)
parsed = parse_qs(urlparse(urls[0]).query)

assert parsed["assets"] == ["B04", "B05"]          # both values preserved
assert parsed["max_size"] == ["512"]               # scalar pass-through OK
assert "step" not in parsed                        # timeseries param stripped
assert parsed["collection_concept_id"] == ["C123"] # deduplicated to model value
```

### Example test scenario

```python
scope = {
    "type": "http",
    "method": "GET",
    "path": "/rasterio/timeseries/statistics",
    "query_string": b"collection_concept_id=C123&sort_key=cloud_cover&max_size=256",
    "headers": [],
}
request = Request(scope)
param_list = [
    GranuleSearch(
        collection_concept_id="C123",
        temporal="2024-01-01T00:00:00Z/2024-01-01T23:59:59Z",
        sort_key=["cloud_cover"],
    ),
]

urls = build_request_urls("http://testserver/stats", request, param_list)
parsed = parse_qs(urlparse(urls[0]).query)

assert parsed["collection_concept_id"] == ["C123"]         # once, from model
assert "sort_key" not in parsed or parsed["sort_key"] == ["cloud_cover"]  # once
assert parsed["max_size"] == ["256"]                    # pass-through preserved
```

## Decision Log

| Decision | Options Considered | Rationale |
|----------|-------------------|-----------|
| Derive exclusion set from `GranuleSearch.model_fields` | Hard-code a static list of field names | Using `model_fields` guarantees new fields are handled automatically, without needing to update `build_request_urls`. |
| Strip only canonical field names, not aliases | Enumerate `validation_alias` values too | Alias stripping would require inspecting `AliasChoices` / `AliasPath` objects, adding complexity with marginal benefit — the lower-level endpoints already understand aliases. |
| Build URL from scratch instead of concatenating lists | Use a dict merge then flatten | The list concatenation approach is simple, explicit, and preserves order. We just need to filter the first list correctly. |
| Use `urlencode(..., doseq=True)` for list expansion | Manually expand lists into string fragments | `doseq=True` is the stdlib mechanism designed for exactly this use case; avoids hand-rolled URL encoding. |

## Open Questions

1. Should we also exclude `concept_id` and `datetime` (legacy aliases) from pass-through params to avoid benign redundancy?  Lower-level endpoints tolerate them, so the impact is low, but it would be tidier.
2. Are there any other `List[str]` fields in dependency models used by `param_list` that might be affected by the `str()` fix?  Only `sort_key` and `attribute` on `GranuleSearch` are currently list-valued.

## Status

- [x] Identified
- [ ] Approved — ready to plan
- [ ] Implementing
- [ ] Implemented

## References

- `titiler/cmr/timeseries.py` — `build_request_urls`
- `titiler/cmr/models.py` — `GranuleSearch`
- `titiler/cmr/dependencies.py` — `GranuleSearchParams`
- `tests/test_timeseries.py` — `test_build_request_urls_no_duplicate_params`
- Related FastAPI docs: [Query Parameters with List](https://fastapi.tiangolo.com/tutorial/query-params-str-validations/#query-parameter-list-multiple-values)
- `urllib.parse.urlencode` with `doseq=True`
