from typing import Any, Dict, List, Literal, Optional

from dlt.common.jsonpath import TJsonPath
from dlt.common.typing import TypedDict

SearchFormatType = Literal["short", "long", "full", "standard"]


class SearchConfig(TypedDict, total=False):
    """Configuration for search queries in the fylr API.

    This TypedDict defines the structure of search configurations. The 'search' field
    is effectively required (not marked Optional), while all other fields are optional.

    Note: total=False makes all fields optional from TypedDict's perspective, but the
    'search' field should always be provided when constructing search queries.

    Reference: https://docs.easydb.de/en/technical/api/search
    """

    type: Optional[str]
    objecttypes: Optional[List[str]]
    search: List[Dict[str, Any]]
    offset: Optional[int]
    best_mask_filter: Optional[bool]
    generate_rights: Optional[bool]
    limit: Optional[int]
    format: Optional[SearchFormatType] | Optional[str]
    language: Optional[str]
    sort: Optional[List[Dict[str, Any]]]
    aggregations: Optional[Dict[str, Any]]
    highlight: Optional[Dict[str, Any]]
    fields: Optional[List[Dict[str, Any]]]
    include_fields: Optional[List[TJsonPath]]
    exclude_fields: Optional[List[TJsonPath]]
    timezone: Optional[str]
    include_deleted: Optional[bool]


class LinkedObjectConfig(TypedDict):
    """Configuration for linked objects in the fylr API.

    This TypedDict defines the structure for tracking linked objecttypes to detect
    unmarked data changes. Both fields are required.

    Fields:
        path: JSON path to extract system object IDs from the linked objecttype's search results.
        search: SearchConfig for querying the linked objecttype and identifying changed objects.
    """

    path: TJsonPath
    search: SearchConfig


class ReverseNestedObjectConfig(TypedDict):
    """Configuration for reverse nested objects in the fylr API.

    Specifies the reverse nested objecttype to fetch in full, the desired output
    format, and the JSON path to splice it into. All fields are required.
    """

    name: str
    format: SearchFormatType | str
    path: TJsonPath


class ObjecttypeConfig(TypedDict, total=False):
    """Configuration for objecttype extraction in the fylr API.

    This TypedDict defines the structure for objecttype configurations. The 'name' and
    'search' fields are required in practice (not marked Optional), while other fields
    are truly optional.

    Note: total=False makes all fields optional from TypedDict's perspective, but the
    fylr_resources function requires 'name' and 'search' to be present.
    """

    name: str
    search: SearchConfig
    linked_objecttypes: Optional[List[LinkedObjectConfig]]
    ignore_references: Optional[bool]
    include_reverse_nested_objects: Optional[List[ReverseNestedObjectConfig]]
    primary_key: Optional[str]
    write_disposition: Optional[Literal["append", "replace", "merge"]]
    max_table_nesting: Optional[int]


class ClientConfig(TypedDict):
    """Configuration for the fylr API client.

    Holds the authentication credentials and the base URL of the fylr instance.
    """

    base_url: str
    client_id: str
    client_secret: str
    username: str
    password: str


class IncrementalValue(TypedDict):
    """Configuration for incremental values in the fylr API.

    Holds the initial value seeding incremental extraction.
    """

    initial_value: str


class FylrConfig(TypedDict, total=False):
    """Configuration for the fylr API source.

    This TypedDict defines the structure for the overall fylr source configuration.
    The 'client' and 'objecttypes' fields are required in practice (not marked Optional),
    while 'incremental' is truly optional.

    Note: total=False makes all fields optional from TypedDict's perspective, but the
    fylr_resources function requires 'client' and 'objecttypes' to be present.
    """

    client: ClientConfig
    objecttypes: List[ObjecttypeConfig]
    incremental: Optional[IncrementalValue]
