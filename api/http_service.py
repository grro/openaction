from abc import ABC, abstractmethod
from typing import Any
from requests import Response


class HttpClient(ABC):

    @abstractmethod
    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        pass

    @abstractmethod
    def get(self, url: str, **kwargs: Any) -> Response:
        pass

    @abstractmethod
    def post(self, url: str, **kwargs: Any) -> Response:
        pass
