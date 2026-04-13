"""titiler.cmr custom exceptions."""


class S3CredentialsEndpointMissing(Exception):
    """No S3 credentials link"""


class NoDataGranule(Exception):
    """No DataGranule slot in granule metadata"""


class InvalidMediaType(Exception):
    """Invalid media type"""


class InvalidBBox(Exception):
    """Invalid bounding box coordinates."""


class InvalidDatetime(Exception):
    """Invalid datetime."""


class CMRQueryTimeout(Exception):
    """CMR granule search request timed out."""
