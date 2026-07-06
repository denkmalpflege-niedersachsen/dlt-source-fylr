"""Reusable dlt source building blocks for the fylr (easydb) REST API.

See the README for full documentation and usage examples.
"""

from .auth import FylrCredentials, OAuth2PasswordCredentials
from .helpers import (
    create_client,
    create_incremental_object,
    create_paginator,
    create_resource,
    expand_incremental_placeholders,
    fetch_objects_by_ids,
    fylr_resources,
    get_linked_object_ids,
    get_objects_data,
    init_search,
    paginate_search,
    validate_config,
)
from .settings import (
    API_DB_ENDPOINT,
    API_SEARCH_ENDPOINT,
    DLT_MAX_TABLE_NESTING,
    INCREMENTAL_CURSOR_PATH_FIELD,
    INCREMENTAL_PLACEHOLDER,
    INCREMENTAL_VALUE_DEFAULT_ATTRIBUTE,
    MAX_PAGE_SIZE,
    MIN_PAGE_SIZE,
    SEARCH_TIMEZONE,
)
from .typing import (
    ClientConfig,
    FylrConfig,
    IncrementalValue,
    LinkedObjectConfig,
    ObjecttypeConfig,
    ReverseNestedObjectConfig,
    SearchConfig,
    SearchFormatType,
)

__all__ = [
    # high-level entrypoint
    "fylr_resources",
    # auth
    "FylrCredentials",
    "OAuth2PasswordCredentials",
    # config typing
    "FylrConfig",
    "ClientConfig",
    "ObjecttypeConfig",
    "SearchConfig",
    "LinkedObjectConfig",
    "ReverseNestedObjectConfig",
    "IncrementalValue",
    "SearchFormatType",
    # lower-level helpers (advanced use)
    "create_client",
    "create_resource",
    "create_paginator",
    "create_incremental_object",
    "expand_incremental_placeholders",
    "fetch_objects_by_ids",
    "get_linked_object_ids",
    "get_objects_data",
    "init_search",
    "paginate_search",
    "validate_config",
    # settings
    "API_SEARCH_ENDPOINT",
    "API_DB_ENDPOINT",
    "MIN_PAGE_SIZE",
    "MAX_PAGE_SIZE",
    "INCREMENTAL_CURSOR_PATH_FIELD",
    "INCREMENTAL_VALUE_DEFAULT_ATTRIBUTE",
    "INCREMENTAL_PLACEHOLDER",
    "SEARCH_TIMEZONE",
    "DLT_MAX_TABLE_NESTING",
]
