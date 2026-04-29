from abc import ABC, abstractmethod
from typing import Any
from requests import Response


class HttpClient(ABC):
    """
    Defines a standardized interface for HTTP clients, commonly used to access
    REST APIs or HTTP-based hardware like Shelly devices.

    This wrapper encapsulates communication with remote servers while managing
    the connection lifecycle internally (e.g., session handling and pooling).

    Note: Client instances are typically dedicated to specific tasks and are
    not intended to be shared across concurrent executions to ensure isolation.
    """

    @abstractmethod
    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        """
        Sends a generic HTTP request.

        Args:
            method (str): The HTTP method (e.g., "GET", "POST", "PUT", "DELETE").
            url (str): The target destination URL.
            **kwargs (Any): Arguments passed to the underlying engine, such as
                'headers', 'params', 'json', or 'timeout'.

        Returns:
            Response: A `requests.Response` object containing the server's reply.
        """
        pass

    @abstractmethod
    def get(self, url: str, **kwargs: Any) -> Response:
        """
        Sends an HTTP GET request. A convenience wrapper for request("GET", ...).

        Args:
            url (str): The target destination URL.
            **kwargs (Any): Optional arguments like 'params' or 'headers'.

        Returns:
            Response: The server's response.
        """
        pass

    @abstractmethod
    def post(self, url: str, **kwargs: Any) -> Response:
        """
        Sends an HTTP POST request. A convenience wrapper for request("POST", ...).

        Args:
            url (str): The target destination URL.
            **kwargs (Any): Optional arguments like 'json', 'data', or 'headers'.

        Returns:
            Response: The server's response.
        """
        pass