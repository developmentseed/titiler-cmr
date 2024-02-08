# API Specification

## API Specification for /tiles/x/y/z Endpoint

## Endpoint Description

`GET /tiles/{tileMatrixSetId}/{z}/{x}/{y}@{scale}x`

`GET /tiles/{tileMatrixSetId}/{z}/{x}/{y}@{scale}x.{format}`

`GET /tiles/{tileMatrixSetId}/{z}/{x}/{y}.{format}`

`GET /tiles/{tileMatrixSetId}/{z}/{x}/{y}`

This endpoint provides tiled data for specific geographical locations and times. Tiles are defined by their x, y, and z coordinates.

## Parameters

- **Path Parameters:**
  - `tileMatrixSetId` (string): TileMatrixSet name (e.g **WebMercatorQuad**)
  - `x` (integer): The x coordinate of the tile
  - `y` (integer): The y coordinate of the tile
  - `z` (integer): The zoom level of the tile
  - `scale` (integer, optional): Tile size scale, default is set to 1 (256x256)
  - `format` (string, optional): Output image format, default is set to None and will be either JPEG or PNG depending on the presence of masked value.

- **Query Parameters:**
  - `concept_id` (string): The [concept ID](https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#c-concept-id) of the collection. **REQUIRED**
  - `temporal` (string, optional): Either a date-time or an interval. Date and time expressions adhere to 'YYYY-MM-DD' format. Intervals may be bounded or half-bounded (double-dots at start or end) **RECOMMENDED**
  - `backend` (*rasterio* or *xarray*, optional): Backend to use in order to read the CMR dataset. Defaults to `rasterio`
  - `variable`* (string, optional): The variable of interest. `required` when using `xarray` backend
  - `time_slice`* (string, optional): The time for which data is requested, in ISO 8601 format
  - `decode_times`* (bool, optional): Whether to decode times
  - `bidx`** (int, optional): Dataset band indexes (multiple allowed)
  - `expression`** (string, optional): rio-tiler's band math expression
  - `unscale`** (bool, optional): Apply dataset internal Scale/Offset.
  - `nodata` (string or number, optional): Overwrite internal Nodata value
  - `resampling`**: RasterIO resampling algorithm. Defaults to `nearest`.
  - `reproject`: WarpKernel resampling algorithm (only used when doing re-projection). Defaults to `nearest`.
  - `algorithm` (string, optional): Custom algorithm name (e.g hillshade).
  - `algorithm_params` (string): JSON encoded algorithm parameters.
  - `color_formula` (string): rio-color formula.
  - `colormap_name` (string, optional): The name of the colormap to apply
  - `colormap` (string, optional): JSON encoded custom Colormap
  - `rescale` (string, optional): The rescale range in the format `min,max`
  - `return_mask` (bool, optional): Add mask to the output data. Defaults to `True`

\* used in `xarray` backend only

\** used in `cog` backend only

## Request Example

GET /tiles/WebMercatorQuad/1/2/3?backend=xarray&variable=temperature&timestamp=2024-01-16T00:00:00Z&colormap=viridis&rescale=0,100&temporal=2024-01-16/2024-01-16&concept_id=C0000000000-YOCLOUD


## Responses

- **200 OK:**
  - Description: Successful response with tile data.
  - Content type: `image/png`
  - Body: [Binary Data]

- **400 Bad Request:**
  - Description: The request is invalid. This can happen with missing required parameters or invalid parameter values.
  - Content type: `application/json`
  - Body:
    ```json
    {
      "error": "Invalid request parameters."
    }
    ```

- **404 Not Found:**
  - Description: No data found for the specified parameters.
  - Content type: `application/json`
  - Body:
    ```json
    {
      "error": "No data found for the provided coordinates and time."
    }
    ```

- **500 Internal Server Error:**
  - Description: Generic server error message for when an error has occurred on the server.
  - Content type: `application/json`
  - Body:
    ```json
    {
      "error": "Internal server error."
    }
    ```


