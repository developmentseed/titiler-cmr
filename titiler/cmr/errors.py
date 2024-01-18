"""tipg.errors: Error classes."""

import logging

from starlette import status

from titiler.core.errors import TilerError

logger = logging.getLogger(__name__)


class InvalidBBox(TilerError):
    """Invalid bounding box coordinates."""


class InvalidDatetime(TilerError):
    """Invalid datetime."""


class MissingCollectionCatalog(TilerError):
    """`collection_catalog` not registered in the application state."""


DEFAULT_STATUS_CODES = {
    InvalidBBox: status.HTTP_422_UNPROCESSABLE_ENTITY,
    InvalidDatetime: status.HTTP_422_UNPROCESSABLE_ENTITY,
    MissingCollectionCatalog: status.HTTP_500_INTERNAL_SERVER_ERROR,
}
