"""fylr API and dlt pipeline configuration settings.

This module defines constants used throughout the fylr data extraction pipeline, including:
- API endpoint paths for fylr REST API communication
- Pagination limits to control API request sizes and prevent overload
- Incremental loading configuration for tracking data changes
- dlt-specific settings for controlling data normalization behavior

These constants are designed to work with the fylr API (https://docs.easydb.de/en/technical/api/)
and the dlt data loading framework (https://dlthub.com/).
"""

API_SEARCH_ENDPOINT = "api/v1/search"  # post endpoint for searching and retrieving objects
API_DB_ENDPOINT = "api/v1/db"  # endpoint for direct database operations (future use)

# default number of records per API request page, balancing request count against response size
MIN_PAGE_SIZE = 100

# maximum allowed records per page; exceeding this raises a ValueError in create_paginator()
MAX_PAGE_SIZE = 1000

# fylr's built-in '_last_modified' timestamp field, used to track changed records
INCREMENTAL_CURSOR_PATH_FIELD = "_last_modified"

# attribute where dlt stores the last successfully loaded incremental cursor value
INCREMENTAL_VALUE_DEFAULT_ATTRIBUTE = "start_value"

# template variable replaced with the actual incremental value by expand_incremental_placeholders()
INCREMENTAL_PLACEHOLDER = "{{incremental}}"

# timezone for interpreting timestamps in fylr API requests, matching the NFIS system
SEARCH_TIMEZONE = "Europe/Berlin"

# maximum depth for normalizing nested JSON structures into relational tables
DLT_MAX_TABLE_NESTING = 3
