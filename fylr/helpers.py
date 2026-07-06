from datetime import datetime
from functools import lru_cache
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Literal,
    Mapping,
    Optional,
    Set,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

import dlt
from dlt.common.jsonpath import TJsonPath, find_values, set_value_at_path
from dlt.extract import DltResource
from dlt.sources import TDataItems, incremental
from dlt.sources.helpers.rest_client.client import PageData, RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator

from .auth import OAuth2PasswordCredentials
from .settings import (
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
    FylrConfig,
    LinkedObjectConfig,
    ReverseNestedObjectConfig,
    SearchConfig,
    SearchFormatType,
)


@lru_cache(maxsize=None)
def _type_hints_for(typed_dict_class) -> Dict[str, Any]:
    """Return the resolved type hints for a TypedDict class, memoized per class.

    The returned mapping is shared across callers, so treat it as read-only.
    """
    return get_type_hints(typed_dict_class)


def validate_config(config: Mapping[str, Any], typed_dict_class) -> None:
    """Validate a dictionary against a TypedDict schema.

    Checks that `config` has all required keys, no unexpected keys, and that
    every value matches its TypedDict-declared type annotation, including
    nested TypedDicts and generic types such as `Optional[T]`, `List[T]`,
    `Dict[K, V]`, and `Union` types.

    Args:
        config: The dictionary to validate against the TypedDict schema.
        typed_dict_class: The TypedDict class defining the expected schema,
            including field names and their type annotations.

    Raises:
        ValueError: If required keys are missing or unexpected keys are present.
        TypeError: If a value's type doesn't match the expected type annotation.

    Example:
        >>> from typing import TypedDict, Optional
        >>> class MyConfig(TypedDict):
        ...     name: str
        ...     count: int
        ...     description: Optional[str]
        ...
        >>> valid_config = {"name": "example", "count": 42, "description": None}
        >>> validate_config(valid_config, MyConfig)  # No error
        >>>
        >>> invalid_config = {"name": "example"}  # Missing 'count'
        >>> validate_config(invalid_config, MyConfig)
        Traceback (most recent call last):
        ...
        ValueError: Missing required keys: {'count'}
    """

    def check_type(value: Any, expected_type: Any, key: str) -> None:
        """Check whether a value matches the expected type annotation."""
        if expected_type is Any:
            return
        origin = get_origin(expected_type)
        # optional[T] is union[T, None]
        if origin is Union:
            args = get_args(expected_type)
            if type(None) in args:
                if value is None:
                    return
                non_none_types = [arg for arg in args if arg is not type(None)]
                for arg_type in non_none_types:
                    try:
                        check_type(value, arg_type, key)
                        return
                    except TypeError as e:
                        # re-raise a nested validation failure immediately instead of
                        # trying other union members
                        error_msg = str(e)
                        if (
                            "validation failed" in error_msg
                            or "list item" in error_msg
                            or "dict validation" in error_msg
                        ):
                            raise
                        continue
                # none of the union members matched
                raise TypeError(
                    f"Key '{key}': expected {expected_type}, got {type(value).__name__}"
                )
            else:
                # regular union (not optional)
                for arg_type in args:
                    try:
                        check_type(value, arg_type, key)
                        return
                    except TypeError as e:
                        # re-raise a nested validation failure immediately instead of
                        # trying other union members
                        error_msg = str(e)
                        if (
                            "validation failed" in error_msg
                            or "list item" in error_msg
                            or "dict validation" in error_msg
                        ):
                            raise
                        continue
                raise TypeError(
                    f"Key '{key}': expected one of {args}, got {type(value).__name__}"
                )
        elif origin is Literal:
            allowed_values = get_args(expected_type)
            if value not in allowed_values:
                raise TypeError(
                    f"Key '{key}': expected one of {allowed_values}, got {value!r}"
                )
        elif origin is list:
            if not isinstance(value, list):
                raise TypeError(
                    f"Key '{key}': expected list, got {type(value).__name__}"
                )
            args = get_args(expected_type)
            if args:
                item_type = args[0]
                for i, item in enumerate(value):
                    try:
                        check_type(item, item_type, f"{key}[{i}]")
                    except (ValueError, TypeError) as e:
                        raise TypeError(
                            f"Key '{key}': list item validation failed: {e}"
                        )
        elif origin is dict:
            if not isinstance(value, dict):
                raise TypeError(
                    f"Key '{key}': expected dict, got {type(value).__name__}"
                )
            args = get_args(expected_type)
            if args and len(args) == 2:
                key_type, value_type = args
                for k, v in value.items():
                    try:
                        check_type(k, key_type, f"{key}[key]")
                        check_type(v, value_type, f"{key}[{k}]")
                    except (ValueError, TypeError) as e:
                        raise TypeError(f"Key '{key}': dict validation failed: {e}")
        else:
            # a TypedDict exposes __annotations__ and __required_keys__; recurse into it
            if hasattr(expected_type, "__annotations__") and hasattr(
                expected_type, "__required_keys__"
            ):
                if not isinstance(value, dict):
                    raise TypeError(
                        f"Key '{key}': expected dict (TypedDict), got {type(value).__name__}"
                    )
                try:
                    validate_config(value, expected_type)
                except (ValueError, TypeError) as e:
                    raise TypeError(
                        f"Key '{key}': nested TypedDict validation failed: {e}"
                    )
            else:
                if not isinstance(value, expected_type):
                    raise TypeError(
                        f"Key '{key}': expected {expected_type.__name__}, got {type(value).__name__}"
                    )

    hints = _type_hints_for(typed_dict_class)
    required_keys = getattr(typed_dict_class, "__required_keys__", set(hints.keys()))
    missing = required_keys - config.keys()
    if missing:
        raise ValueError(f"Missing required keys: {missing}")
    extra = config.keys() - hints.keys()
    if extra:
        raise ValueError(f"Unexpected keys: {extra}")
    for key, expected_type in hints.items():
        if key in config:
            value = config[key]
            check_type(value, expected_type, key)


def create_paginator(
    mode: Literal["db", "search"] = "search", page_size: int = MIN_PAGE_SIZE
) -> OffsetPaginator:
    """Create a Paginator for paginating through results from the fylr API.

    Initializes an OffsetPaginator with the specified page size and the
    appropriate configuration for the fylr API.

    Args:
        mode: The pagination mode to use. Either 'db' for database endpoints or 'search'
            for search endpoints. Defaults to 'search'. The mode determines whether
            pagination parameters are sent as query params ('db') or in the request body ('search').
        page_size: The number of items to fetch per page. Must be a positive integer
            and cannot exceed 1000 to avoid overwhelming the API. Defaults to MIN_PAGE_SIZE (100).

    Returns:
        An instance of OffsetPaginator configured for the fylr API.

    Raises:
        ValueError: If mode is not 'db' or 'search', if page_size is not a positive integer,
            or if page_size exceeds 1000.

    Example:
        >>> paginator = create_paginator(mode="search", page_size=100)
        >>> paginator.limit
        100
    """
    if mode is not None and mode not in ["db", "search"]:
        raise ValueError("mode must be either 'db' or 'search'")
    if not isinstance(page_size, int) or page_size <= 0:
        raise ValueError("page_size must be a positive integer")
    if page_size > MAX_PAGE_SIZE:
        raise ValueError(
            f"page_size cannot exceed {MAX_PAGE_SIZE} to avoid overwhelming the API"
        )
    match mode:
        case "db":
            return OffsetPaginator(
                limit=page_size,
                limit_param="limit",
                offset_param="offset",
                total_path="count",
            )
        case "search":
            return OffsetPaginator(
                limit=page_size,
                limit_body_path="limit",
                offset_body_path="offset",
                total_path="count",
            )


def create_client(
    base_url: str,
    client_id: str,
    client_secret: str,
    username: str,
    password: str,
    page_size: Optional[int] = None,
) -> RESTClient:
    """Create a configured RESTClient for the fylr API.

    Initializes a RESTClient with OAuth2 password grant authentication,
    offset-based pagination, and the appropriate data selectors for the fylr
    API, ready to interact with a fylr instance.

    Args:
        base_url: The base URL of the fylr instance (e.g., "https://example.fylr.io").
        client_id: OAuth2 client identifier for authentication.
        client_secret: OAuth2 client secret for authentication.
        username: Username for OAuth2 password grant authentication.
        password: Password for OAuth2 password grant authentication.
        page_size: Optional number of items to fetch per page. If not provided, defaults to 100. Can't exceed 1000 to avoid overwhelming the API.

    Returns:
        A fully configured RESTClient instance ready to make authenticated requests
        to the fylr API with automatic pagination support.

    Raises:
        ValueError: If any required parameter is missing, if page_size is invalid,
            or if RESTClient construction fails.

    Example:
        >>> client = create_client(
        ...     base_url="https://nfis.gbv.de",
        ...     client_id="my_client_id",
        ...     client_secret="my_secret",
        ...     username="user@example.com",
        ...     password="secure_password"
        ... )
        >>> response = client.post("api/v1/search", json={"objecttypes": ["item"]})
    """
    if not all([base_url, client_id, client_secret, username, password]):
        raise ValueError(
            "All parameters (base_url, client_id, client_secret, username, password) must be provided"
        )
    if page_size is not None and (not isinstance(page_size, int) or page_size <= 0):
        raise ValueError("page_size must be a positive integer if provided")
    try:
        return RESTClient(
            base_url=base_url,
            auth=OAuth2PasswordCredentials(
                access_token_url=f"{base_url}/api/oauth2/token",
                client_id=client_id,
                client_secret=client_secret,
                access_token_request_data={
                    "scope": "offline",
                    "grant_type": "password",
                    "username": username,
                    "password": password,
                },
            ),
            data_selector="objects",
            paginator=create_paginator(page_size=page_size or MIN_PAGE_SIZE),
        )
    except Exception as e:
        raise ValueError(f"Failed to create RESTClient: {e}") from e


def paginate_search(
    client: RESTClient,
    search: SearchConfig,
    validate: bool = True,
) -> Generator[PageData[Any], None, None]:
    """Generate paginated results from the fylr API.

    Yields pages of search results from the fylr API search endpoint (/api/v1/search).
    This function abstracts the pagination logic, allowing callers to simply iterate
    over the results without worrying about handling pagination manually.

    Args:
        client: RESTClient instance configured for fylr API.
        search: SearchConfig dictionary with objecttypes, format, and search criteria.
        validate: Whether to validate `search` against the SearchConfig schema before
            paginating. Defaults to True. Pass False for searches built internally (e.g.
            via `init_search`) that are already well-formed, to skip redundant validation
            on hot paths.

    Yields:
        PageData objects, each containing a batch of search results.

    Raises:
        ValueError: If client/search missing or search config invalid.

    Example:
        >>> search = init_search("objekt", format="long")
        >>> for page in paginate_search(client, search):
        ...     for item in page:
        ...         print(item["_system_object_id"])
    """
    if not client:
        raise ValueError("A RESTClient instance must be provided for pagination")
    if not search:
        raise ValueError("A search query must be provided for pagination")
    # skip validation for an internally-built (already well-formed) search
    if validate:
        try:
            validate_config(search, SearchConfig)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid search configuration: {e}") from e
    if client.paginator is None:
        try:
            client.paginator = create_paginator(mode="search", page_size=MIN_PAGE_SIZE)
        except ValueError as e:
            raise ValueError(f"Failed to create paginator: {e}") from e
    try:
        yield from client.paginate(
            path=API_SEARCH_ENDPOINT, method="POST", json=cast(Dict[str, Any], search)
        )
    except Exception as e:
        raise ValueError(
            f"Pagination failed for endpoint '{API_SEARCH_ENDPOINT}': {e}"
        ) from e


def get_linked_object_ids(
    client: RESTClient,
    search: SearchConfig,
    path: TJsonPath,
) -> Generator[int, None, None]:
    """Yield integer IDs found at a JSON path across paginated search results.

    Args:
        client: RESTClient instance to use for API requests.
        search: Search query dictionary to send to the API.
        path: JSON path to extract values from the search results. For example,
            "objekt__bild.lk_objekt._system_object_id" extracts the system object
            ID from nested linked objects.

    Yields:
        Integer IDs found at the specified path in the search results. Items
        without values at the specified path are skipped (when find_values returns empty list).

    Raises:
        ValueError: If a non-integer or None value is found at the specified path.

    Example:
        >>> search = {"search": [...], "objecttypes": ["objekt__bild"]}
        >>> path = "objekt__bild.lk_objekt._system_object_id"
        >>> ids = set(get_linked_object_ids(client, search, path))
        >>> len(ids)  # Number of unique IDs found
        42
    """

    for page in paginate_search(client, search, validate=False):
        for item in page:
            values = find_values(path, item)
            for value in values:
                if value is not None and isinstance(value, int):
                    yield value
                else:
                    raise ValueError(
                        f"Expected integer ID at path '{path}', but got: {value}"
                    )


def init_search(objecttype: str, format: SearchFormatType = "long") -> SearchConfig:
    """Initialize a fylr API search query configuration.

    Creates a baseline search query dictionary with default settings for querying
    the fylr API. The query includes object type selection, sorting,
    and formatting options.

    Args:
        objecttype: The object type to search for (e.g., 'objekt').
            This is used for both filtering and field selection.
        format: The format for search results. Must be one of 'long', 'full', 'short',
            or 'standard'. Controls the level of detail in the returned object data.

    Returns:
        A dictionary containing the complete search configuration with:
        - object type specification
        - specified format for results
        - ascending sort by system object ID
        - timezone set to Europe/Berlin (can be changed in the settings.py file)

    Raises:
        ValueError: If objecttype is empty, if format is not a valid SearchFormatType,
            or if SearchConfig construction fails.

    Example:
        >>> search = init_search('objekt', 'long')
        >>> search['objecttypes']
        ['objekt']
        >>> search['format']
        'long'
    """
    if not objecttype:
        raise ValueError("objecttype must be provided to initialize search query")
    if format not in ["long", "full", "short", "standard"]:
        raise ValueError("format must be one of 'long', 'full', 'short', 'standard'")
    try:
        return SearchConfig(
            objecttypes=[objecttype],
            format=format,
            sort=[{"field": "_system_object_id", "order": "ASC", "_level": 0}],
            timezone=SEARCH_TIMEZONE,
        )
    except Exception as e:
        raise ValueError(f"Failed to create SearchConfig: {e}") from e


@dlt.defer
def get_objects_data(
    client: RESTClient,
    objecttype: str,
    system_object_ids: List[int],
    format: SearchFormatType = "long",
) -> List[Dict[str, Any]]:
    """Fetch object data for a batch of system object IDs in a single search.

    Issues one paginated `_system_object_id IN (...)` search instead of one request
    per ID, collapsing an N+1 pattern into `ceil(len/MIN_PAGE_SIZE)` requests. The
    `@dlt.defer` decorator lets dlt resolve multiple batches in parallel via its
    thread pool.

    Args:
        client: RESTClient instance configured for fylr API communication.
        objecttype: The object type to retrieve (e.g., 'objekt').
        system_object_ids: System object IDs to fetch. Must be a non-empty list of
            positive integers, and should not exceed MIN_PAGE_SIZE per call.
        format: The format for the response data. Must be one of 'long', 'full',
            'short', or 'standard'. Defaults to 'long'.

    Returns:
        A list of dictionaries with the object data for the requested IDs. When used
        with @dlt.defer, returns a deferred value that dlt will resolve.

    Raises:
        ValueError: If required parameters are missing or invalid, if the API
            request fails, or if any requested system object ID is not returned.
    """
    if not client:
        raise ValueError("A RESTClient instance must be provided")
    if not objecttype:
        raise ValueError("objecttype must be provided to fetch object data")
    if not system_object_ids:
        raise ValueError("system_object_ids must be provided and cannot be empty")
    if any(not isinstance(sid, int) or sid <= 0 for sid in system_object_ids):
        raise ValueError("system_object_ids must all be positive integers")
    if format not in ["long", "full", "short", "standard"]:
        raise ValueError("format must be one of 'long', 'full', 'short', 'standard'")

    # one search fetching all requested IDs at once
    search = init_search(objecttype, format)
    search["search"] = [
        {
            "type": "in",
            "bool": "must",
            "fields": ["_system_object_id"],
            "in": list(system_object_ids),
        }
    ]
    # search is built internally via init_search, so skip re-validation
    data: List[Dict[str, Any]] = []
    try:
        for page in paginate_search(client, search, validate=False):
            data.extend(page)
    except Exception as e:
        raise ValueError(
            f"API request failed for {objecttype} batch of "
            f"{len(system_object_ids)} ids: {e}"
        ) from e
    # require every requested ID to come back; a short result means an object was
    # deleted or became inaccessible since its ID was collected, which we surface
    # rather than silently dropping
    found_ids = {obj.get("_system_object_id") for obj in data}
    missing = [sid for sid in system_object_ids if sid not in found_ids]
    if missing:
        raise ValueError(
            f"No data found for {len(missing)} of {len(system_object_ids)} "
            f"requested {objecttype} objects (missing _system_object_ids: {missing})"
        )
    return data


def fetch_objects_by_ids(
    client: RESTClient,
    objecttype: str,
    object_ids: List[int],
) -> Generator[List[Dict], None, None]:
    """Fetch object data for multiple object IDs in batched, parallel requests.

    Chunks `object_ids` into batches of MIN_PAGE_SIZE and yields one deferred
    `get_objects_data` call per chunk. dlt resolves the chunks in parallel via its
    thread pool, so the whole set is fetched in `ceil(len/MIN_PAGE_SIZE)` requests
    instead of one request per ID.

    Args:
        client: RESTClient instance configured for fylr API communication.
        objecttype: The object type to retrieve (e.g., 'objekt').
        object_ids: List of system object IDs to fetch. Must be a non-empty list
            of positive integers.

    Yields:
        Lists of dictionaries containing object data, one list per chunk. When
        resolved by dlt, the chunks are fetched in parallel.

    Raises:
        ValueError: If required parameters are missing or invalid (e.g., empty
            object_ids list, missing client, or objecttype).

    Example:
        >>> # Within a dlt resource:
        >>> @dlt.resource
        >>> def my_resource(client):
        >>>     ids = [12345, 12346, 12347]
        >>>     yield from fetch_objects_by_ids(client, "objekt", ids)
    """
    if not client:
        raise ValueError("A RESTClient instance must be provided")
    if not objecttype:
        raise ValueError("objecttype must be provided to fetch object data")
    if not object_ids:
        raise ValueError("object_ids list must be provided and cannot be empty")

    for start in range(0, len(object_ids), MIN_PAGE_SIZE):
        chunk = object_ids[start : start + MIN_PAGE_SIZE]
        # get_objects_data is @dlt.defer, so each chunk resolves in parallel
        yield cast(List[Dict], get_objects_data(client, objecttype, chunk))


def is_valid_date_format(date_string: str) -> bool:
    """Validate a date string against YYYY-MM-DD format.

    Checks if the provided date string conforms to the ISO 8601 date format
    (YYYY-MM-DD) and can be successfully parsed as a valid date. This function
    is useful for validating incremental loading start dates and other date
    parameters before they are used in API queries.

    Args:
        date_string: Date string to validate. Expected format is 'YYYY-MM-DD'
            (e.g., '2024-01-25'). Only string types are accepted.

    Returns:
        True if the date string is in valid YYYY-MM-DD format and represents
        a valid calendar date, False otherwise. Returns False for invalid types,
        malformed strings, or invalid dates (e.g., '2024-02-30').

    Example:
        >>> is_valid_date_format('2024-01-25')
        True
        >>> is_valid_date_format('2024-13-01')  # Invalid month
        False
        >>> is_valid_date_format('2024/01/25')  # Wrong format
        False
        >>> is_valid_date_format(20240125)  # Not a string
        False
    """
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def create_incremental_object(incremental_value: str) -> incremental:
    """Create a dlt incremental object for tracking last modified timestamps.

    Initializes a dlt incremental loading configuration that tracks changes based
    on the '_last_modified' field. This enables efficient incremental loading by only
    extracting records that have been modified since the last pipeline run.
    The initial value is validated as YYYY-MM-DD before the incremental object
    is created, ensuring the fylr API receives properly formatted date filters.

    Args:
        incremental_value: Initial date value in YYYY-MM-DD format (e.g., '2024-01-25').
            This represents the starting point for incremental loading. Records with
            '_last_modified' timestamps greater than or equal to this value will be
            extracted. The value is converted to ISO 8601 format with timezone (T00:00:00Z).

    Returns:
        A dlt incremental object configured to track the '_last_modified' field
        with the specified initial value (converted to ISO 8601 format).

    Raises:
        ValueError: If incremental_value is not in valid YYYY-MM-DD format,
            represents an invalid date, or if incremental object creation fails.

    Example:
        >>> inc = create_incremental_object('2024-01-01')
        >>> inc.cursor_path
        '_last_modified'
        >>> inc.initial_value
        '2024-01-01T00:00:00Z'
        >>> create_incremental_object('2024-13-01')  # Invalid month
        Traceback (most recent call last):
        ...
        ValueError: Invalid incremental_initial_value format: 2024-13-01. Expected 'YYYY-MM-DD'.
    """
    if incremental_value is not None:
        if not is_valid_date_format(incremental_value):
            raise ValueError(
                f"Invalid incremental_initial_value format: {incremental_value}. Expected 'YYYY-MM-DD'."
            )
    try:
        return incremental(
            cursor_path=INCREMENTAL_CURSOR_PATH_FIELD,
            initial_value=f"{incremental_value}T00:00:00Z",
        )
    except Exception as e:
        raise ValueError(f"Failed to create incremental object: {e}") from e


def expand_incremental_placeholders(
    config: SearchConfig, incremental_value: Any
) -> None:
    """Replace incremental placeholders in a SearchConfig with the actual incremental value.

    Every string matching the incremental placeholder (e.g., "{{incremental}}"),
    at any depth, is replaced with `incremental_value`, allowing the current value
    to be injected into a search query before it is sent to the fylr API. The
    config is modified in-place.

    Args:
        config: The SearchConfig dictionary to modify in-place.
        incremental_value: The actual value to replace the placeholder with. This is
            typically obtained from a dlt incremental object and represents the last
            modified timestamp to use for filtering search results.

    Returns:
        None. The config dictionary is modified in-place.

    Example:
        >>> config = {
        ...     "search": [
        ...         {
        ...             "type": "range",
        ...             "bool": "must",
        ...             "fields": ["_last_modified"],
        ...             "from": "{{incremental}}"
        ...         }
        ...     ],
        ...     "objecttypes": ["objekt"]
        ... }
        >>> expand_incremental_placeholders(config, "2024-01-01T00:00:00Z")
        >>> print(config["search"][0]["from"])
        2024-01-01T00:00:00Z
    """

    def recursive_replace(obj: Any) -> Any:
        """Recursively replace placeholders, returning the (possibly replaced) value."""
        if isinstance(obj, dict):
            dict_obj = cast(Dict[str, Any], obj)
            for key, value in dict_obj.items():
                dict_obj[key] = recursive_replace(value)
            return dict_obj
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                obj[i] = recursive_replace(item)
            return obj
        elif isinstance(obj, str):
            return incremental_value if obj == INCREMENTAL_PLACEHOLDER else obj
        else:
            return obj

    config_dict = cast(Dict[str, Any], config)
    for key in config_dict:
        config_dict[key] = recursive_replace(config_dict[key])


def create_resource(
    client: RESTClient,
    objecttype: str,
    search: SearchConfig,
    ignore_references: bool = False,
    linked_objecttypes: Optional[List[LinkedObjectConfig]] = None,
    incremental_object: Optional[incremental[str]] = None,
) -> Generator[TDataItems, None, None]:
    """Create a dlt resource for loading fylr objects with reference tracking and incremental loading.

    The resource yields:
    - Paginated search results from the fylr API
    - Incrementally filtered records based on the '_last_modified' field
    - Linked objects from related objecttypes
    - Child objects for parent-child relationships
    - Objects modified only via linked objecttypes (unmarked data changes)

    Related objects are captured even when they were not directly modified but are
    referenced by objects that were, keeping complex object hierarchies consistent.

    Args:
        client: A configured RESTClient instance for making authenticated requests
            to the fylr API.
        objecttype: The fylr objecttype to extract (e.g., 'objekt', 'objekt__bild').
            Used to construct search queries and identify object relationships.
        search: A SearchConfig dictionary containing the search query configuration.
            Should include objecttypes, format, and optional search filters. Supports
            incremental placeholder '{{incremental}}' that will be replaced with the
            current incremental value.
        ignore_references: If True, disables reference tracking and child object fetching.
            Set to True for simple extractions that don't need to follow object relationships.
            Defaults to False.
        linked_objecttypes: Optional list of LinkedObjectConfig dictionaries specifying
            related objecttypes to track for unmarked changes. Each config must include:
            - 'search': SearchConfig for querying the linked objecttype
            - 'path': JSONPath to extract system object IDs from linked objects
            When provided, the function tracks which objects from these linked types
            were modified and ensures all references to them are also extracted.
        incremental_object: Optional dlt incremental object for tracking the last
            extraction state. When provided, automatically expands '{{incremental}}'
            placeholders in search queries with the last modified timestamp, enabling
            efficient incremental loads.

    Yields:
        Pages of object data as TDataItems. Each yield returns a list of objects
        (either a search result page, child objects page, or individual unmarked objects).
        The yielded data can be directly consumed by dlt for loading into the destination.

    Raises:
        ValueError: If required parameters are missing or invalid, if the search config
            is malformed, if linked_objecttype configs are missing required fields,
            or if object extraction fails at any stage.

    Example:
        >>> # Basic usage without incremental loading or reference tracking
        >>> client = create_client(...)
        >>> search = init_search("objekt", format="long")
        >>> for page in create_resource(client, "objekt", search, ignore_references=True):
        ...     print(f"Loaded {len(page)} objects")

        >>> # With incremental loading and linked objecttype tracking
        >>> inc_obj = create_incremental_object("2024-01-01")
        >>> search["search"] = [{
        ...     "type": "range",
        ...     "fields": ["_last_modified"],
        ...     "from": "{{incremental}}"
        ... }]
        >>> linked = [{
        ...     "search": init_search("objekt__bild"),
        ...     "path": "objekt__bild.lk_objekt._system_object_id"
        ... }]
        >>> yield from create_resource(
        ...     client, "objekt", search,
        ...     linked_objecttypes=linked,
        ...     incremental_object=inc_obj
        ... )
    """

    if not client:
        raise ValueError("A RESTClient instance must be provided")
    if not objecttype:
        raise ValueError("objecttype must be provided")
    if not search:
        raise ValueError("A SearchConfig must be provided")

    if client.paginator is None:
        try:
            client.paginator = create_paginator(mode="search", page_size=MIN_PAGE_SIZE)
        except ValueError as e:
            raise ValueError(f"Failed to create paginator: {e}") from e

    try:
        validate_config(search, SearchConfig)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid search configuration: {e}") from e

    incremental_value = None

    if incremental_object:
        # dlt updates this value after each run, driving incremental loads on later runs
        incremental_value = getattr(
            incremental_object, INCREMENTAL_VALUE_DEFAULT_ATTRIBUTE, None
        )
        if incremental_value:
            expand_incremental_placeholders(search, incremental_value)

    if not ignore_references:
        unmarked_data_changes: Set[int] = set()

        if linked_objecttypes:
            for linked_objecttype in linked_objecttypes:
                path = linked_objecttype.get("path")
                if not path:
                    raise ValueError("Each linked_objecttype must have a 'path' field")

                linked_objecttype_search = linked_objecttype.get("search")
                if not linked_objecttype_search:
                    raise ValueError(
                        f"Linked objecttype '{objecttype}' must contain a 'search' field"
                    )

                if incremental_value:
                    expand_incremental_placeholders(
                        linked_objecttype_search, incremental_value
                    )

                try:
                    unmarked_data_changes.update(
                        get_linked_object_ids(client, linked_objecttype_search, path)
                    )
                except ValueError as e:
                    raise ValueError(
                        f"Failed to get linked objecttype IDs for path '{path}': {e}"
                    ) from e

    # `search` was already validated above, so skip re-validation per page. (Validation
    # runs before `expand_incremental_placeholders`; the post-expansion form is not
    # re-checked, which is safe while expansion only substitutes scalar values.)
    for page in paginate_search(client, search, validate=False):
        if not ignore_references:
            try:
                system_object_ids = set()
                parent_object_ids = set()
                for item in page:
                    if (sid := item.get("_system_object_id")) is not None:
                        system_object_ids.add(sid)
                    # only items with children can be parents
                    if item.get("_has_children"):
                        if obj_type := item.get("_objecttype"):
                            if (oid := item.get(obj_type, {}).get("_id")) is not None:
                                parent_object_ids.add(oid)
            except (KeyError, TypeError, AttributeError) as e:
                raise ValueError(
                    f"Failed to extract system object IDs and parent object IDs from page data: {e}"
                ) from e
            if unmarked_data_changes:
                unmarked_data_changes -= system_object_ids
        yield page

        if not ignore_references and parent_object_ids:
            # fetch child objects referencing the parents found on this page
            search_for_child_objects = init_search(objecttype, format="long")
            search_for_child_objects["search"] = [
                {
                    "type": "in",
                    "bool": "must",
                    "fields": [f"{objecttype}._parents.{objecttype}._id"],
                    "in": list(parent_object_ids),
                }
            ]
            try:
                # search is built via init_search, so it needs no validation
                child_system_ids = set()
                for child_page in paginate_search(
                    client, search_for_child_objects, validate=False
                ):
                    for child in child_page:
                        if (child_sid := child.get("_system_object_id")) is not None:
                            child_system_ids.add(child_sid)
                    yield child_page
                if unmarked_data_changes and child_system_ids:
                    unmarked_data_changes -= child_system_ids
            except ValueError as e:
                raise ValueError(f"Failed to fetch child objects: {e}") from e
    # fetch objects that changed only via linked objecttypes and weren't yielded above
    if not ignore_references and unmarked_data_changes:
        try:
            yield from fetch_objects_by_ids(
                client, objecttype, list(unmarked_data_changes)
            )
        except (ValueError, TypeError) as e:
            raise ValueError(f"Failed to fetch remaining objects: {e}") from e


def fylr_resources(
    config: FylrConfig, yield_limit: Optional[int] = None
) -> List[DltResource]:
    """Create a list of dlt resources for extracting data from the fylr API.

    This function creates dlt resources for each object type in the fylr configuration. Each resource
    is configured with its own search query and optional linked object tracking for
    reconciling unmarked data changes and child objects.

    The function validates the provided configuration, initializes an authenticated
    REST client, and returns a dlt resource for each object type configuration.

    When reverse nested objects are configured for an objecttype, the base resource name
    gets a "_base" suffix (e.g., "item" becomes "item_base"), and a transformer resource
    with the original name is created to enrich the data.

    Args:
        config: FylrConfig dictionary containing the complete configuration with three main sections:
            - 'client': Client configuration dictionary with authentication and connection details:
                * 'base_url': Base URL of the fylr instance (e.g., "https://nfis.gbv.de")
                * 'client_id': OAuth2 client ID for authentication
                * 'client_secret': OAuth2 client secret for authentication
                * 'username': Username for OAuth2 password grant authentication
                * 'password': Password for OAuth2 password grant authentication
            - 'objecttypes': List of ObjecttypeConfig dictionaries, each containing:
                * 'name': The resource/object type name (e.g., 'item', 'flaeche')
                * 'search': SearchConfig dictionary with search query parameters
                * 'ignore_references': (Optional) Boolean to disable linked object and child
                  object processing. Defaults to False. When True, only main search results
                  are extracted, improving performance when references aren't needed.
                * 'linked_objecttypes': (Optional) List of linked object configurations for
                  tracking unmarked data changes. Only processed when ignore_references is False.
                  Each configuration includes:
                    - 'path': JSON path to extract parent system object IDs from linked objecttype results
                    - 'search': SearchConfig for querying the linked objecttype
                * 'include_reverse_nested_objects': (Optional) List of reverse nested object configurations
                  that specify object types to be fully fetched and embedded instead of just referenced. Each configuration includes:
                    - 'name': The name for the reverse nested objecttype resource (must start with '_reverse_nested:')
                    - 'path': JSON path to extract system object IDs from the reverse nested data
                    - 'format': The format to use for the search query when fetching the full object data
                * 'primary_key': (Optional) Primary key column(s) for the resource (and its
                  reverse-nested transformer). Defaults to '_system_object_id'.
                * 'write_disposition': (Optional) dlt write disposition for the resource (and its
                  reverse-nested transformer): 'append', 'replace', or 'merge'. Defaults to 'replace'.
                * 'max_table_nesting': (Optional) Maximum JSON nesting depth normalized into
                  child tables for the resource (and its reverse-nested transformer).
                  Defaults to DLT_MAX_TABLE_NESTING.
            - 'incremental': (Optional) Incremental loading configuration dictionary:
                * 'initial_value': The initial value for incremental loading (required if
                  incremental section is provided)
        yield_limit: Optional maximum number of items to yield per resource. If None or <= 0,
            no limit is applied. Useful for testing or limiting data extraction. When specified,
            each resource will stop yielding after the limit is reached.

    Returns:
        List[DltResource]: A list of dlt.resource instances for each configured object type,
        ready to be run in a dlt pipeline. Each resource yields paginated data from the
        fylr API with automatic handling of authentication and pagination.

    Raises:
        ValueError: If the configuration is invalid (missing required fields, type
            mismatches), if client creation fails, or if any objecttype configuration
            is invalid (missing name or search fields).

    Example:
        >>> import dlt
        >>> from fylr import fylr_resources
        >>>
        >>> # Define the complete fylr configuration
        >>> config = {
        ...     "client": {
        ...         "base_url": "https://nfis.gbv.de",
        ...         "client_id": "my_client_id",
        ...         "client_secret": "my_client_secret",
        ...         "username": "user@example.com",
        ...         "password": "secure_password"
        ...     },
        ...     "objecttypes": [
        ...         {
        ...             "name": "item",
        ...             "search": {
        ...                 "objecttypes": ["item"],
        ...                 "format": "long"
        ...             },
        ...             "ignore_references": True  # Simplified extraction without references
        ...         },
        ...         {
        ...             "name": "flaeche",
        ...             "search": {
        ...                 "objecttypes": ["flaeche"],
        ...                 "format": "long"
        ...             },
        ...             "ignore_references": False,  # Enable full reference tracking
        ...             "linked_objecttypes": [
        ...                 {
        ...                     "path": "flaeche__bild.lk_flaeche._system_object_id",
        ...                     "search": {
        ...                         "objecttypes": ["flaeche__bild"],
        ...                         "format": "long"
        ...                     }
        ...                 }
        ...             ],
                        "include_reverse_nested_objects": [
                            {
                                "name": "_reverse_nested:flaeche_subtype:lk_flaeche",
                                "path": "flaeche.subtype._system_object_id",
                                "format": "long"
                            }
                        ]
        ...         }
        ...     ],
        ...     "incremental": {
        ...         "initial_value": "2023-01-01"
        ...     }
        ... }
        >>>
        >>> # Create the resources with the configuration
        >>> resources = fylr_resources(config=config)
        >>>
        >>> # Run in a pipeline
        >>> pipeline = dlt.pipeline(
        ...     pipeline_name="nfis",
        ...     destination="duckdb",
        ...     dataset_name="nfis_data"
        ... )
        >>> pipeline.run(resources)
    """
    try:
        validate_config(config, FylrConfig)
    except Exception as e:
        raise ValueError(f"Invalid configuration: {e}") from e

    client_config = config.get("client")

    if not client_config or not all(
        key in client_config
        for key in ("base_url", "client_id", "client_secret", "username", "password")
    ):
        raise ValueError(
            "Client configuration must include 'base_url', 'client_id', 'client_secret', 'username', and 'password'"
        )
    try:
        client = create_client(
            **client_config,
        )
    except Exception as e:
        raise ValueError(f"Failed to create client: {e}")

    objecttypes = config.get("objecttypes")

    if not objecttypes:
        raise ValueError("Objecttypes configuration must be provided")

    incremental_config = config.get("incremental")
    if incremental_config:
        initial_value = incremental_config.get("initial_value", False)
        if not initial_value:
            raise ValueError(
                "Incremental configuration must include 'initial_value' field"
            )
        incremental_object = create_incremental_object(initial_value)

    resources = []

    for objecttype in objecttypes:
        name = objecttype.get("name")
        if not name:
            raise ValueError("Each resource must have a 'name' field")

        resource_name = name  # suffixed later if reverse nested objects are included

        search = objecttype.get("search")
        if not search:
            raise ValueError(f"Objecttype '{name}' must have a 'search' configuration")

        # per-objecttype overrides for the dlt resource/transformer, falling back
        # to the source-wide defaults (replace, on the fylr system object id, nested
        # up to DLT_MAX_TABLE_NESTING). Resolved once here and reused for both the
        # base resource and the reverse-nested transformer so they stay in sync.
        write_disposition = objecttype.get("write_disposition") or "replace"
        primary_key = objecttype.get("primary_key") or "_system_object_id"
        max_table_nesting = objecttype.get("max_table_nesting") or DLT_MAX_TABLE_NESTING

        resource_params: Dict[str, Any] = {
            "client": client,
            "objecttype": name,
            "search": search,
            "ignore_references": objecttype.get("ignore_references", False),
        }

        if (
            not resource_params["ignore_references"]
            and "linked_objecttypes" in objecttype
        ):
            resource_params["linked_objecttypes"] = objecttype["linked_objecttypes"]

        if incremental_config and incremental_object:
            resource_params["incremental_object"] = incremental_object

        if reverse_nested_objects := objecttype.get("include_reverse_nested_objects"):
            # the transformer keeps the plain name; the base resource is suffixed
            resource_name = f"{name}_base"

        try:
            resource = dlt.resource(
                create_resource,
                name=resource_name,
                section="fylr",
                write_disposition=write_disposition,
                primary_key=primary_key,
                max_table_nesting=max_table_nesting,
                parallelized=True,  # parallel processing for large datasets
            )(**resource_params)
        except Exception as e:
            raise ValueError(f"Failed to create resource '{name}': {e}")

        if yield_limit is not None and yield_limit > 0:
            resource.add_limit(yield_limit)

        if reverse_nested_objects:
            # a factory captures the loop variables per iteration for the closure below
            def create_transformer(
                objecttype: str,
                config: List[ReverseNestedObjectConfig],
                write_disposition: Literal["append", "replace", "merge"],
                primary_key: str,
                max_table_nesting: int,
            ):
                """Build a dlt transformer for `objecttype`, capturing it and `config` in the
                transformer's own closure scope to avoid the loop-variable capture bug that
                would occur if the transformer referenced the loop variables directly.
                """

                @dlt.transformer(
                    name=objecttype,
                    section="fylr",
                    max_table_nesting=max_table_nesting,
                    write_disposition=write_disposition,
                    primary_key=primary_key,
                    parallelized=True,  # parallel processing for large datasets
                )
                def collect_reverse_nested_objects(items: TDataItems) -> TDataItems:
                    """Enrich `items` by replacing reverse-nested object references (identified
                    by the '_reverse_nested:' prefix) with the full fetched object data.

                    Note: IDs are batched per config across all items, so the API is queried
                    once per config (chunked by MIN_PAGE_SIZE) rather than once per item.
                    """

                    BASE_PATH = "_reverse_nested:"

                    # materialize so we can make two passes (collect IDs, then enrich)
                    # over the same items; the dicts are shared by reference, so the
                    # in-place enrichment below still mutates the original objects
                    items = list(items)

                    def get_reverse_nested_data(
                        item: Dict[str, Any], objecttype_name: str
                    ) -> Any:
                        """Return the reverse-nested reference list for `objecttype_name`
                        from an item, using the item's own `_objecttype` to locate it."""
                        item_objecttype = item.get("_objecttype")
                        if not item_objecttype:
                            raise ValueError(
                                "Each item must have an '_objecttype' field"
                            )
                        return item[item_objecttype].get(objecttype_name)

                    # process each reverse nested config once for the whole batch of items
                    for reverse_nested_object in config:
                        objecttype_name = reverse_nested_object.get("name")
                        data_path = reverse_nested_object.get("path")
                        objecttype_format = reverse_nested_object.get("format")

                        # the target objecttype is the last segment of the data path
                        reverse_nested_objecttype = cast(str, data_path).split(".")[-1]

                        if not objecttype_name.startswith(BASE_PATH):
                            raise ValueError(
                                f"Reverse nested objecttype name '{objecttype_name}' must start with '{BASE_PATH}'"
                            )

                        # pass 1: collect the unique referenced object IDs across all items
                        all_object_ids = set()
                        for item in items:
                            reverse_nested_data = get_reverse_nested_data(
                                item, objecttype_name
                            )
                            if not reverse_nested_data:
                                continue
                            for reference in reverse_nested_data:
                                nested_data = find_values(data_path, reference)
                                if nested_data:
                                    obj_id = nested_data[0].get("_id")
                                    if obj_id:
                                        all_object_ids.add(obj_id)

                        if not all_object_ids:
                            continue

                        # fetch the full object data for all collected IDs in a
                        # chunked search (one request per MIN_PAGE_SIZE ids), building a
                        # shared lookup map keyed by object id
                        objects_by_id: Dict[Any, Any] = {}
                        all_ids = list(all_object_ids)
                        try:
                            for start in range(0, len(all_ids), MIN_PAGE_SIZE):
                                chunk = all_ids[start : start + MIN_PAGE_SIZE]
                                search = init_search(
                                    reverse_nested_objecttype,
                                    cast(SearchFormatType, objecttype_format),
                                )
                                search["search"] = [
                                    {
                                        "type": "in",
                                        "bool": "must",
                                        "fields": [f"{reverse_nested_objecttype}._id"],
                                        "in": chunk,
                                    }
                                ]
                                # search is built via init_search, so skip re-validation
                                for page in paginate_search(
                                    client, search, validate=False
                                ):
                                    for obj in page:
                                        obj_id = obj.get(
                                            reverse_nested_objecttype, {}
                                        ).get("_id")
                                        if obj_id is not None:
                                            objects_by_id[obj_id] = obj
                        except Exception as e:
                            raise ValueError(
                                f"API request failed for reverse nested objecttype '{objecttype_name}': {e}"
                            ) from e

                        # pass 2: enrich every item's references from the shared lookup
                        for item in items:
                            reverse_nested_data = get_reverse_nested_data(
                                item, objecttype_name
                            )
                            if not reverse_nested_data:
                                continue
                            for reference in reverse_nested_data:
                                nested_data = find_values(data_path, reference)
                                if nested_data:
                                    obj_id = nested_data[0].get("_id")
                                    if obj_id and obj_id in objects_by_id:
                                        set_value_at_path(
                                            reference,
                                            cast(str, data_path),
                                            objects_by_id[obj_id],
                                        )

                    return items

                return collect_reverse_nested_objects

            transformer = create_transformer(
                name,
                reverse_nested_objects,
                write_disposition,
                primary_key,
                max_table_nesting,
            )

            try:
                resource = resource | transformer

            except Exception as e:
                raise ValueError(
                    f"Failed to apply reverse nested object transformer to resource '{name}': {e}"
                )

        resources.append(resource)

    if not resources:
        raise ValueError("No valid resources could be created from the configuration")
    return resources
