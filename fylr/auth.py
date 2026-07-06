from typing import Dict

import dlt
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.typing import TSecretValue
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials


@configspec
class FylrCredentials(BaseConfiguration):
    """OAuth2 password-grant credentials for the fylr API.

    Grouping these fields into a configspec prevents dlt from falling back
    to bare environment-variable names when resolving the nested `sources.fylr.credentials.*` keys.

    Configure via `.dlt/secrets.toml` under the `[sources.fylr.credentials]`
    section, or via prefixed environment variables such as
    `SOURCES__FYLR__CREDENTIALS__USERNAME`.
    """

    client_id: TSecretValue = dlt.secrets.value
    client_secret: TSecretValue = dlt.secrets.value
    username: TSecretValue = dlt.secrets.value
    password: TSecretValue = dlt.secrets.value


@configspec
class OAuth2PasswordCredentials(OAuth2ClientCredentials):
    """OAuth2 authentication using the password grant type.

    Extends `OAuth2ClientCredentials` to authenticate with a username and password
    in addition to the client credentials required by the fylr API.

    Attributes:
        Inherits all attributes from OAuth2ClientCredentials including:
        - client_id: OAuth2 client identifier
        - client_secret: OAuth2 client secret
        - access_token_request_data: Additional data for token requests (e.g., username, password, grant_type)

    Example:
        >>> auth = OAuth2PasswordCredentials(
        ...     access_token_url="https://api.example.com/oauth2/token",
        ...     client_id="my_client_id",
        ...     client_secret="my_client_secret",
        ...     access_token_request_data={
        ...         "grant_type": "password",
        ...         "username": "user@example.com",
        ...         "password": "secret",
        ...         "scope": "offline"
        ...     }
        ... )
        >>> client = RESTClient(base_url="https://api.example.com", auth=auth)
    """

    def build_access_token_request(self) -> Dict:
        return {
            "headers": {
                "Content-Type": "application/x-www-form-urlencoded",
            },
            "data": {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                **self.access_token_request_data,
            },
        }
