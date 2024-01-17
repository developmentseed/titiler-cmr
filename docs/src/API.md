# API Specification

## API Specification for /tiles/x/y/z Endpoint

## Endpoint Description

`GET /tiles/{x}/{y}/{z}`

This endpoint provides tiled data for specific geographical locations and times. Tiles are defined by their x, y, and z coordinates.

## Parameters

- **Path Parameters:**
  - `x` (integer): The x coordinate of the tile.
  - `y` (integer): The y coordinate of the tile.
  - `z` (integer): The zoom level of the tile.

- **Query Parameters:**
  - `collection_concept_id` (string, required): The concept ID of the collection.
  - `variable` (string, required): The variable of interest.
  - `timestamp` (string, required): The time for which data is requested, in ISO 8601 format.
  - `colormap` (string, optional): The name of the colormap to apply.
  - `rescale` (string, optional): The rescale range in the format `min,max`.

## Request Example

GET /tiles/1/2/3?collection_concept_id=C0000000000-YOCLOUD&variable=temperature&timestamp=2024-01-16T00:00:00Z&colormap=viridis&rescale=0,100


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


