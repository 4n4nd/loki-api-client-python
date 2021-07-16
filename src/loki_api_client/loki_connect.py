"""
A Class for collection of logs from a Loki host.

References:
    - https://requests.readthedocs.io/
    - https://grafana.com/docs/loki/latest/api/
"""

import logging
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

__author__ = "Anand Sanmukhani"
__copyright__ = "Anand Sanmukhani"
__license__ = "MIT"

# set up logging
_logger = logging.getLogger(__name__)

# In case of a connection failure try 2 more times
MAX_REQUEST_RETRIES = 3
# wait 1 second before retrying in case of an error
RETRY_BACKOFF_FACTOR = 1
# retry only on these status
RETRY_ON_STATUS = [408, 429, 500, 502, 503, 504]


class LokiConnect:
    """
    A Class for collection of metrics from a Loki Host.

    Args:
      url (str): url for the loki host
      headers (dict): A dictionary of http headers to be used to communicate with
        the host. Example: {"Authorization": "bearer my_oauth_token_to_the_host"}
      disable_ssl (bool): If set to True, will disable ssl certificate verification
        for the http requests made to the host
      retry (Retry): Retry adapter to retry on HTTP errors
    """

    def __init__(
        self,
        url: str = "http://127.0.0.1:3100",
        headers: dict = None,
        disable_ssl: bool = False,
        retry: Retry = None,
        ignore_http_errors: bool = False,
    ):
        """Functions as a Constructor for the class LokiConnect."""
        if url is None:
            raise TypeError("missing url")

        self.headers = headers
        self.url = url
        self.loki_host = urlparse(self.url).netloc
        self._all_metrics = None
        self.ssl_verification = not disable_ssl
        self.ignore_http_errors = ignore_http_errors

        if retry is None:
            retry = Retry(
                total=MAX_REQUEST_RETRIES,
                backoff_factor=RETRY_BACKOFF_FACTOR,
                status_forcelist=RETRY_ON_STATUS,
            )

        self._session = requests.Session()
        self._session.mount(self.url, HTTPAdapter(max_retries=retry))

    def ready(self, params: dict = None) -> bool:
        """
        Check if Loki host is ready to accept traffic.
        Ref: https://grafana.com/docs/loki/latest/api/#get-ready

        Args:
            params (dict): Optional dictionary containing parameters to be
                sent along with the API request.

        Returns:
            bool: True if the host is ready, False if the endpoint is not ready.
        """
        params = params or {}

        response = self._session.get(
            "{0}/ready".format(self.url),
            verify=self.ssl_verification,
            headers=self.headers,
            params=params,
        )
        return response.ok

    def query(
        self,
        query: str,
        limit: int = 10,
        time: str = None,
        direction: str = "backward",
        params: dict = None,
    ) -> dict:
        """
        Check if Loki host is ready to accept traffic.
        Ref: https://grafana.com/docs/loki/latest/api/#query

        Args:
            query (str): The LogQL query to perform.
            limit (int): The max number of entries to return.
            time (str): The evaluation time for the query as a nanosecond Unix epoch.
                        Defaults to now.
            direction (str): Determines the sort order of logs. Supported values are
                        "forward" or "backward". Defaults to "backward".
            params (dict): Optional dictionary containing parameters to be
                sent along with the API request.

        Returns:
            dict: A json of queried log data.
        """
        params = params or {}

        if query:
            if not isinstance(query, str):
                raise TypeError(
                    "Incorrect query type: {}, should be type: {}".format(
                        type(query), str
                    )
                )
            params["query"] = query
        else:
            raise ValueError("query empty")

        if time:
            params["time"] = time

        if limit:
            params["limit"] = limit

        direction_supported_values = ["backward", "forward"]
        if direction not in direction_supported_values:
            raise ValueError("Invalid direction Value: {}".format(direction))
        params["direction"] = direction

        response = self._session.get(
            "{0}/loki/api/v1/query".format(self.url),
            verify=self.ssl_verification,
            headers=self.headers,
            params=params,
        )

        if not self.ignore_http_errors:
            response.raise_for_status()

        return response.json()
