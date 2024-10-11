"""ogcapi pydantic models."""

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, Union

from pydantic import AnyHttpUrl, AnyUrl, BaseModel, Field, RootModel
from typing_extensions import Annotated

from titiler.cmr.enums import MediaType


class Link(BaseModel):
    """Link model.

    Ref: https://github.com/opengeospatial/ogcapi-tiles/blob/master/openapi/schemas/common-core/link.yaml

    Code generated using https://github.com/koxudaxi/datamodel-code-generator/
    """

    href: Annotated[
        str,
        Field(
            description="Supplies the URI to a remote resource (or resource fragment).",
            examples=["http://data.example.com/buildings/123"],
        ),
    ]
    rel: Annotated[
        str,
        Field(
            description="The type or semantics of the relation.", examples=["alternate"]
        ),
    ]
    type: Annotated[
        Optional[MediaType],
        Field(
            description="A hint indicating what the media type of the result of dereferencing the link should be.",
            examples=["application/geo+json"],
        ),
    ] = None
    templated: Annotated[
        Optional[bool],
        Field(description="This flag set to true if the link is a URL template."),
    ] = None
    varBase: Annotated[
        Optional[str],
        Field(
            description="A base path to retrieve semantic information about the variables used in URL template.",
            examples=["/ogcapi/vars/"],
        ),
    ] = None
    hreflang: Annotated[
        Optional[str],
        Field(
            description="A hint indicating what the language of the result of dereferencing the link should be.",
            examples=["en"],
        ),
    ] = None
    title: Annotated[
        Optional[str],
        Field(
            description="Used to label the destination of a link such that it can be used as a human-readable identifier.",
            examples=["Trierer Strasse 70, 53115 Bonn"],
        ),
    ] = None
    length: Optional[int] = None

    model_config = {"use_enum_values": True}


class CRSUri(BaseModel):
    """Coordinate Reference System (CRS) from URI."""

    uri: Annotated[
        AnyUrl,
        Field(
            description="Reference to one coordinate reference system (CRS) as URI",
            examples=[
                "http://www.opengis.net/def/crs/EPSG/0/3978",
                "urn:ogc:def:crs:EPSG::2193",
            ],
        ),
    ]


class CRSWKT(BaseModel):
    """Coordinate Reference System (CRS) from WKT."""

    wkt: Annotated[
        str,
        Field(
            description="Reference to one coordinate reference system (CRS) as WKT string",
            examples=[
                'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]',
            ],
        ),
    ]


class CRSRef(BaseModel):
    """CRS from referenceSystem."""

    referenceSystem: Dict[str, Any] = Field(
        ...,
        description="A reference system data structure as defined in the MD_ReferenceSystem of the ISO 19115",
    )


class CRS(RootModel[Union[str, Union[CRSUri, CRSWKT, CRSRef]]]):
    """CRS model.

    Ref: https://github.com/opengeospatial/ogcapi-tiles/blob/master/openapi/schemas/common-geodata/crs.yaml

    Code generated using https://github.com/koxudaxi/datamodel-code-generator/
    """


class Spatial(BaseModel):
    """Spatial Extent model.

    Ref: http://schemas.opengis.net/ogcapi/features/part1/1.0/openapi/schemas/extent.yaml

    """

    # Bbox
    # One or more bounding boxes that describe the spatial extent of the dataset.
    # The first bounding box describes the overall spatial
    # extent of the data. All subsequent bounding boxes describe
    # more precise bounding boxes, e.g., to identify clusters of data.
    bbox: List[List[float]]
    crs: str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"


class Temporal(BaseModel):
    """Temporal Extent model.

    Ref: http://schemas.opengis.net/ogcapi/features/part1/1.0/openapi/schemas/extent.yaml

    """

    # The first time interval describes the overall
    # temporal extent of the data. All subsequent time intervals describe
    # more precise time intervals, e.g., to identify clusters of data.
    # Clients only interested in the overall temporal extent will only need
    # to access the first time interval in the array (a pair of lower and upper
    # bound instants).
    interval: List[List[Optional[str]]]
    trs: str = "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"


class Extent(BaseModel):
    """Extent model.

    Ref: http://schemas.opengis.net/ogcapi/features/part1/1.0/openapi/schemas/extent.yaml

    """

    spatial: Optional[Spatial] = None
    temporal: Optional[Temporal] = None


class Collection(BaseModel):
    """Collection model.

    Note: `CRS` is the list of CRS supported by the service not the CRS of the collection

    Ref: http://schemas.opengis.net/ogcapi/features/part1/1.0/openapi/schemas/collection.yaml

    """

    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    links: List[Link]
    extent: Optional[Extent] = None
    itemType: Optional[str] = None
    crs: List[str] = ["http://www.opengis.net/def/crs/OGC/1.3/CRS84"]

    model_config = {"extra": "allow"}

    # Custom property
    @property
    def bounds(self) -> Optional[List[float]]:
        """Return spatial bounds from collection extent."""
        if self.extent and self.extent.spatial:
            return self.extent.spatial.bbox[0]

        return None

    # Custom property
    @property
    def dt_bounds(self) -> Optional[List[Optional[str]]]:
        """Return temporal bounds from collection extent."""
        if self.extent and self.extent.temporal:
            return self.extent.temporal.interval[0]

        return None


class Collections(BaseModel):
    """
    Collections model.

    Ref: http://beta.schemas.opengis.net/ogcapi/common/part2/0.1/collections/openapi/schemas/collections.yaml

    """

    links: List[Link]
    timeStamp: Optional[str] = None
    numberMatched: Optional[int] = None
    numberReturned: Optional[int] = None
    collections: List[Collection]

    model_config = {"extra": "allow"}


class Conformance(BaseModel):
    """Conformance model.

    Ref: http://schemas.opengis.net/ogcapi/features/part1/1.0/openapi/schemas/confClasses.yaml

    """

    conformsTo: List[str]


class Landing(BaseModel):
    """Landing page model.

    Ref: http://schemas.opengis.net/ogcapi/features/part1/1.0/openapi/schemas/landingPage.yaml

    """

    title: Optional[str] = None
    description: Optional[str] = None
    links: List[Link]


class Queryables(BaseModel):
    """Queryables model.

    Ref: https://docs.ogc.org/DRAFTS/19-079r1.html#filter-queryables

    """

    title: str
    properties: Dict[str, Dict[str, str]]
    type: str = "object"
    schema_name: Annotated[
        str, Field(alias="$schema")
    ] = "https://json-schema.org/draft/2019-09/schema"
    link: Annotated[str, Field(alias="$id")]

    model_config = {"populate_by_name": True}


class TileMatrixSetLink(BaseModel):
    """
    TileMatrixSetLink model.
    Based on http://docs.opengeospatial.org/per/19-069.html#_tilematrixsets
    """

    href: str
    rel: str = "http://www.opengis.net/def/rel/ogc/1.0/tiling-schemes"
    type: MediaType = MediaType.json

    model_config = {"use_enum_values": True}


class TileMatrixSetRef(BaseModel):
    """
    TileMatrixSetRef model.
    Based on http://docs.opengeospatial.org/per/19-069.html#_tilematrixsets
    """

    id: str
    title: Optional[str] = None
    links: List[TileMatrixSetLink]


class TileMatrixSetList(BaseModel):
    """
    TileMatrixSetList model.
    Based on http://docs.opengeospatial.org/per/19-069.html#_tilematrixsets
    """

    tileMatrixSets: List[TileMatrixSetRef]


class TimeStamp(RootModel):
    """TimeStamp model.

    Ref: https://github.com/opengeospatial/ogcapi-tiles/blob/master/openapi/schemas/common-geodata/timeStamp.yaml

    Code generated using https://github.com/koxudaxi/datamodel-code-generator/
    """

    root: Annotated[
        datetime,
        Field(
            description="This property indicates the time and date when the response was generated using RFC 3339 notation.",
            examples=["2017-08-17T08:05:32Z"],
        ),
    ]


class BoundingBox(BaseModel):
    """BoundingBox model.

    Ref: https://github.com/opengeospatial/ogcapi-tiles/blob/master/openapi/schemas/tms/2DBoundingBox.yaml

    Code generated using https://github.com/koxudaxi/datamodel-code-generator/
    """

    lowerLeft: Annotated[
        List[float],
        Field(
            max_length=2,
            min_length=2,
            description="A 2D Point in the CRS indicated elsewhere",
        ),
    ]
    upperRight: Annotated[
        List[float],
        Field(
            max_length=2,
            min_length=2,
            description="A 2D Point in the CRS indicated elsewhere",
        ),
    ]
    crs: Annotated[Optional[CRS], Field(title="CRS")] = None
    orderedAxes: Annotated[
        Optional[List[str]], Field(max_length=2, min_length=2)
    ] = None


# Ref: https://github.com/opengeospatial/ogcapi-tiles/blob/master/openapi/schemas/tms/propertiesSchema.yaml
Type = Literal["array", "boolean", "integer", "null", "number", "object", "string"]

# Ref: https://github.com/opengeospatial/ogcapi-tiles/blob/master/openapi/schemas/tms/propertiesSchema.yaml
AccessConstraints = Literal[
    "unclassified", "restricted", "confidential", "secret", "topSecret"
]


class Properties(BaseModel):
    """Properties model.

    Ref: https://github.com/opengeospatial/ogcapi-tiles/blob/master/openapi/schemas/tms/propertiesSchema.yaml

    Code generated using https://github.com/koxudaxi/datamodel-code-generator/
    """

    title: Optional[str] = None
    description: Annotated[
        Optional[str], Field(description="Implements 'description'")
    ] = None
    type: Optional[Type] = None
    enum: Annotated[
        Optional[Set],
        Field(
            description="Implements 'acceptedValues'",
            min_length=1,
        ),
    ] = None
    format: Annotated[
        Optional[str],
        Field(description="Complements implementation of 'type'"),
    ] = None
    contentMediaType: Annotated[
        Optional[str], Field(description="Implements 'mediaType'")
    ] = None
    maximum: Annotated[Optional[float], Field(description="Implements 'range'")] = None
    exclusiveMaximum: Annotated[
        Optional[float], Field(description="Implements 'range'")
    ] = None
    minimum: Annotated[Optional[float], Field(description="Implements 'range'")] = None
    exclusiveMinimum: Annotated[
        Optional[float], Field(description="Implements 'range'")
    ] = None
    pattern: Optional[str] = None
    maxItems: Annotated[
        Optional[int],
        Field(
            description="Implements 'upperMultiplicity'",
            ge=0,
        ),
    ] = None
    minItems: Annotated[
        Optional[int],
        Field(
            description="Implements 'lowerMultiplicity'",
            ge=0,
        ),
    ] = 0
    observedProperty: Optional[str] = None
    observedPropertyURI: Optional[AnyUrl] = None
    uom: Optional[str] = None
    uomURI: Optional[AnyUrl] = None


class PropertiesSchema(BaseModel):
    """PropertiesSchema model.

    Ref: https://github.com/opengeospatial/ogcapi-tiles/blob/master/openapi/schemas/tms/propertiesSchema.yaml

    Code generated using https://github.com/koxudaxi/datamodel-code-generator/
    """

    type: Literal["object"]
    required: Annotated[
        Optional[List[str]],
        Field(
            description="Implements 'multiplicity' by citing property 'name' defined as 'additionalProperties'",
            min_length=1,
        ),
    ] = None
    properties: Dict[str, Properties]


class Style(BaseModel):
    """Style model.

    Ref: https://github.com/opengeospatial/ogcapi-tiles/blob/master/openapi/schemas/tms/style.yaml

    Code generated using https://github.com/koxudaxi/datamodel-code-generator/
    """

    id: Annotated[
        str,
        Field(
            description="An identifier for this style. Implementation of 'identifier'"
        ),
    ]
    title: Annotated[Optional[str], Field(description="A title for this style")] = None
    description: Annotated[
        Optional[str], Field(description="Brief narrative description of this style")
    ] = None
    keywords: Annotated[
        Optional[List[str]], Field(description="keywords about this style")
    ] = None
    links: Annotated[
        Optional[List[Link]],
        Field(
            description="Links to style related resources. Possible link 'rel' values are: 'style' for a URL pointing to the style description, 'styleSpec' for a URL pointing to the specification or standard used to define the style.",
            min_length=1,
        ),
    ] = None


class GeospatialData(BaseModel):
    """Geospatial model.

    Ref: https://github.com/opengeospatial/ogcapi-tiles/blob/master/openapi/schemas/tms/geospatialData.yaml

    Code generated using https://github.com/koxudaxi/datamodel-code-generator/
    """

    title: Annotated[
        Optional[str],
        Field(
            description="Title of this tile matrix set, normally used for display to a human",
        ),
    ] = None
    description: Annotated[
        Optional[str],
        Field(
            description="Brief narrative description of this tile matrix set, normally available for display to a human",
        ),
    ] = None
    keywords: Annotated[
        Optional[str],
        Field(
            description="Unordered list of one or more commonly used or formalized word(s) or phrase(s) used to describe this layer",
        ),
    ] = None
    id: Annotated[
        str,
        Field(
            description="Unique identifier of the Layer. Implementation of 'identifier'"
        ),
    ]
    dataType: Annotated[
        Literal["map", "vector", "coverage"],
        Field(description="Type of data represented in the tileset"),
    ]
    geometryDimension: Annotated[
        Optional[int],
        Field(  # type: ignore
            description="The geometry dimension of the features shown in this layer (0: points, 1: curves, 2: surfaces, 3: solids), unspecified: mixed or unknown",
            ge=0,
            le=3,
        ),
    ] = None
    featureType: Annotated[
        Optional[str],
        Field(
            description="Feature type identifier. Only applicable to layers of datatype 'geometries'",
        ),
    ] = None
    attribution: Annotated[
        Optional[str],
        Field(description="Short reference to recognize the author or provider"),
    ] = None
    license: Annotated[
        Optional[str], Field(description="License applicable to the tiles")
    ] = None
    pointOfContact: Annotated[
        Optional[str],
        Field(
            description="Useful information to contact the authors or custodians for the layer (e.g. e-mail address, a physical address,  phone numbers, etc)",
        ),
    ] = None
    publisher: Annotated[
        Optional[str],
        Field(
            description="Organization or individual responsible for making the layer available",
        ),
    ] = None
    theme: Annotated[
        Optional[str], Field(description="Category where the layer can be grouped")
    ] = None
    crs: Annotated[Optional[CRS], Field(title="CRS")] = None
    epoch: Annotated[
        Optional[float],
        Field(description="Epoch of the Coordinate Reference System (CRS)"),
    ] = None
    minScaleDenominator: Annotated[
        Optional[float],
        Field(description="Minimum scale denominator for usage of the layer"),
    ] = None
    maxScaleDenominator: Annotated[
        Optional[float],
        Field(description="Maximum scale denominator for usage of the layer"),
    ] = None
    minCellSize: Annotated[
        Optional[float], Field(description="Minimum cell size for usage of the layer")
    ] = None
    maxCellSize: Annotated[
        Optional[float], Field(description="Maximum cell size for usage of the layer")
    ] = None
    maxTileMatrix: Annotated[
        Optional[str],
        Field(
            description="TileMatrix identifier associated with the minScaleDenominator",
        ),
    ] = None
    minTileMatrix: Annotated[
        Optional[str],
        Field(
            description="TileMatrix identifier associated with the maxScaleDenominator",
        ),
    ] = None
    boundingBox: Optional[BoundingBox] = None
    created: Optional[TimeStamp] = None
    updated: Optional[TimeStamp] = None
    style: Optional[Style] = None
    geoDataClasses: Annotated[
        Optional[List[str]],
        Field(
            description="URI identifying a class of data contained in this layer (useful to determine compatibility with styles or processes)",
        ),
    ] = None
    propertiesSchema: Optional[PropertiesSchema] = None
    links: Annotated[
        Optional[List[Link]],
        Field(
            description="Links related to this layer. Possible link 'rel' values are: 'geodata' for a URL pointing to the collection of geospatial data.",
            min_length=1,
        ),
    ] = None


class TilePoint(BaseModel):
    """TilePoint model.

    Ref: https://github.com/opengeospatial/ogcapi-tiles/blob/master/openapi/schemas/tms/tilePoint.yaml

    Code generated using https://github.com/koxudaxi/datamodel-code-generator/
    """

    coordinates: Annotated[List[float], Field(max_length=2, min_length=2)]
    crs: Annotated[Optional[CRS], Field(title="CRS")]
    tileMatrix: Annotated[
        Optional[str],
        Field(description="TileMatrix identifier associated with the scaleDenominator"),
    ] = None
    scaleDenominator: Annotated[
        Optional[float],
        Field(description="Scale denominator of the tile matrix selected"),
    ] = None
    cellSize: Annotated[
        Optional[float], Field(description="Cell size of the tile matrix selected")
    ] = None


class TileMatrixLimits(BaseModel):
    """
    The limits for an individual tile matrix of a TileSet's TileMatrixSet, as defined in the OGC 2D TileMatrixSet and TileSet Metadata Standard

    Based on https://github.com/opengeospatial/ogcapi-tiles/blob/master/openapi/schemas/tms/tileMatrixLimits.yaml
    """

    tileMatrix: str
    minTileRow: Annotated[int, Field(ge=0)]
    maxTileRow: Annotated[int, Field(ge=0)]
    minTileCol: Annotated[int, Field(ge=0)]
    maxTileCol: Annotated[int, Field(ge=0)]


class TileSet(BaseModel):
    """
    TileSet model.

    Based on https://github.com/opengeospatial/ogcapi-tiles/blob/master/openapi/schemas/tms/tileSet.yaml
    """

    title: Annotated[
        Optional[str], Field(description="A title for this tileset")
    ] = None
    description: Annotated[
        Optional[str], Field(description="Brief narrative description of this tile set")
    ] = None
    dataType: Annotated[
        Literal["map", "vector", "coverage"],
        Field(description="Type of data represented in the tileset"),
    ]
    crs: Annotated[CRS, Field(title="CRS")]
    tileMatrixSetURI: Annotated[
        Optional[AnyUrl],
        Field(
            description="Reference to a Tile Matrix Set on an official source for Tile Matrix Sets"
        ),
    ] = None
    links: Annotated[
        List[Link],
        Field(description="Links to related resources"),
    ]
    tileMatrixSetLimits: Annotated[
        Optional[List[TileMatrixLimits]],
        Field(
            description="Limits for the TileRow and TileCol values for each TileMatrix in the tileMatrixSet. If missing, there are no limits other that the ones imposed by the TileMatrixSet. If present the TileMatrices listed are limited and the rest not available at all",
        ),
    ] = None
    epoch: Annotated[
        Optional[Union[float, int]],
        Field(description="Epoch of the Coordinate Reference System (CRS)"),
    ] = None
    layers: Annotated[
        Optional[List[GeospatialData]],
        Field(min_length=1),
    ] = None
    boundingBox: Optional[BoundingBox] = None
    centerPoint: Optional[TilePoint] = None
    style: Optional[Style] = None
    attribution: Annotated[
        Optional[str],
        Field(description="Short reference to recognize the author or provider"),
    ] = None
    license: Annotated[
        Optional[str], Field(description="License applicable to the tiles")
    ] = None
    accessConstraints: Annotated[
        Optional[AccessConstraints],
        Field(
            description="Restrictions on the availability of the Tile Set that the user needs to be aware of before using or redistributing the Tile Set",
        ),
    ] = "unclassified"
    keywords: Annotated[
        Optional[List[str]], Field(description="keywords about this tileset")
    ] = None
    version: Annotated[
        Optional[str],
        Field(
            description="Version of the Tile Set. Changes if the data behind the tiles has been changed",
        ),
    ] = None
    created: Optional[TimeStamp] = None
    updated: Optional[TimeStamp] = None
    pointOfContact: Annotated[
        Optional[str],
        Field(
            description="Useful information to contact the authors or custodians for the Tile Set",
        ),
    ] = None
    mediaTypes: Annotated[
        Optional[List[str]], Field(description="Media types available for the tiles")
    ] = None


class TileSetList(BaseModel):
    """
    TileSetList model.

    Based on https://docs.ogc.org/is/20-057/20-057.html#toc34
    """

    tilesets: List[TileSet]


axesInfo = Annotated[List[str], Field(min_length=2, max_length=2)]
NumType = Union[float, int]
BoundsType = Tuple[NumType, NumType]


class TMSBoundingBox(BaseModel, arbitrary_types_allowed=True):
    """Bounding box

    ref: https://github.com/opengeospatial/2D-Tile-Matrix-Set/blob/master/schemas/tms/2.0/json/2DBoundingBox.json

    """

    lowerLeft: Annotated[
        BoundsType,
        Field(description="A 2D Point in the CRS indicated elsewhere"),
    ]
    upperRight: Annotated[
        BoundsType,
        Field(description="A 2D Point in the CRS indicated elsewhere"),
    ]
    crs: Annotated[
        Optional[CRS],
        Field(description="Coordinate Reference System (CRS)"),
    ] = None
    orderedAxes: Annotated[
        Optional[axesInfo],
        Field(description="Ordered list of names of the dimensions defined in the CRS"),
    ] = None


class variableMatrixWidth(BaseModel):
    """Variable Matrix Width Definition

    ref: https://github.com/opengeospatial/2D-Tile-Matrix-Set/blob/master/schemas/tms/2.0/json/variableMatrixWidth.json
    """

    coalesce: Annotated[
        int,
        Field(
            ge=2,
            multiple_of=1,
            description="Number of tiles in width that coalesce in a single tile for these rows",
        ),
    ]
    minTileRow: Annotated[
        int,
        Field(
            ge=0,
            multiple_of=1,
            description="First tile row where the coalescence factor applies for this tilematrix",
        ),
    ]
    maxTileRow: Annotated[
        int,
        Field(
            ge=0,
            multiple_of=1,
            description="Last tile row where the coalescence factor applies for this tilematrix",
        ),
    ]


class TileMatrix(BaseModel, extra="forbid"):
    """Tile Matrix Definition

    A tile matrix, usually corresponding to a particular zoom level of a TileMatrixSet.

    ref: https://github.com/opengeospatial/2D-Tile-Matrix-Set/blob/master/schemas/tms/2.0/json/tileMatrix.json
    """

    title: Annotated[
        Optional[str],
        Field(
            description="Title of this tile matrix, normally used for display to a human"
        ),
    ] = None
    description: Annotated[
        Optional[str],
        Field(
            description="Brief narrative description of this tile matrix set, normally available for display to a human",
        ),
    ] = None
    keywords: Annotated[
        Optional[List[str]],
        Field(
            description="Unordered list of one or more commonly used or formalized word(s) or phrase(s) used to describe this dataset",
        ),
    ] = None
    id: Annotated[
        str,
        Field(
            pattern=r"^\-?[0-9]+$",
            description="Identifier selecting one of the scales defined in the TileMatrixSet and representing the scaleDenominator the tile. Implementation of 'identifier'",
        ),
    ]
    scaleDenominator: Annotated[
        float,
        Field(description="Scale denominator of this tile matrix"),
    ]
    cellSize: Annotated[
        float,
        Field(description="Cell size of this tile matrix"),
    ]
    cornerOfOrigin: Annotated[
        Optional[Literal["topLeft", "bottomLeft"]],
        Field(
            description="The corner of the tile matrix (_topLeft_ or _bottomLeft_) used as the origin for numbering tile rows and columns. This corner is also a corner of the (0, 0) tile.",
        ),
    ] = None
    pointOfOrigin: Annotated[
        BoundsType,
        Field(
            description="Precise position in CRS coordinates of the corner of origin (e.g. the top-left corner) for this tile matrix. This position is also a corner of the (0, 0) tile. In previous version, this was 'topLeftCorner' and 'cornerOfOrigin' did not exist.",
        ),
    ]
    tileWidth: Annotated[
        int,
        Field(
            ge=1,
            multiple_of=1,
            description="Width of each tile of this tile matrix in pixels",
        ),
    ]
    tileHeight: Annotated[
        int,
        Field(
            ge=1,
            multiple_of=1,
            description="Height of each tile of this tile matrix in pixels",
        ),
    ]
    matrixWidth: Annotated[
        int,
        Field(
            ge=1,
            multiple_of=1,
            description="Width of the matrix (number of tiles in width)",
        ),
    ]
    matrixHeight: Annotated[
        int,
        Field(
            ge=1,
            multiple_of=1,
            description="Height of the matrix (number of tiles in height)",
        ),
    ]
    variableMatrixWidths: Annotated[
        Optional[List[variableMatrixWidth]],
        Field(description="Describes the rows that has variable matrix width"),
    ] = None


class TileMatrixSet(BaseModel, arbitrary_types_allowed=True):
    """Tile Matrix Set Definition

    A definition of a tile matrix set following the Tile Matrix Set standard.
    For tileset metadata, such a description (in `tileMatrixSet` property) is only required for offline use,
    as an alternative to a link with a `http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme` relation type.

    ref: https://github.com/opengeospatial/2D-Tile-Matrix-Set/blob/master/schemas/tms/2.0/json/tileMatrixSet.json

    """

    title: Annotated[
        Optional[str],
        Field(
            description="Title of this tile matrix set, normally used for display to a human",
        ),
    ] = None
    description: Optional[str] = Field(
        None,
        description="Brief narrative description of this tile matrix set, normally available for display to a human",
    )
    keywords: Annotated[
        Optional[List[str]],
        Field(
            description="Unordered list of one or more commonly used or formalized word(s) or phrase(s) used to describe this tile matrix set",
        ),
    ] = None
    id: Annotated[
        Optional[str],
        Field(
            pattern=r"^[\w\d_\-]+$",
            description="Tile matrix set identifier. Implementation of 'identifier'",
        ),
    ] = None
    uri: Annotated[
        Optional[str],
        Field(description="Reference to an official source for this tileMatrixSet"),
    ] = None
    orderedAxes: Annotated[
        Optional[axesInfo],
        Field(description="Ordered list of names of the dimensions defined in the CRS"),
    ] = None
    crs: Annotated[
        CRS,
        Field(description="Coordinate Reference System (CRS)"),
    ]
    wellKnownScaleSet: Annotated[
        Optional[AnyHttpUrl],
        Field(description="Reference to a well-known scale set"),
    ] = None
    boundingBox: Annotated[
        Optional[TMSBoundingBox],
        Field(
            description="Minimum bounding rectangle surrounding the tile matrix set, in the supported CRS",
        ),
    ] = None
    tileMatrices: Annotated[
        List[TileMatrix],
        Field(description="Describes scale levels and its tile matrices"),
    ]
