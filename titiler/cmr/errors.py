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
    """CMR search request timed out."""


class CMROutage(Exception):
    """CMR search request failed on CMR's side.

    CMR could not be reached, did not answer with a search result, or answered
    with a 3xx or 5xx.
    """
