# titiler-cmr

An API for creating image tiles from CMR queries.

## What's here

* `/stac/collections/` contains STAC collections json. Requests to titiler-cmr should contain all the necessary parameters to query CMR and open a dataset. At this time, that information will be stored as STAC. These files contain examples of what those STAC entries for CMR will look like. But it will be up to clients (such as a UI) to fetch STAC entries and parse them to pass a request to titiler.
