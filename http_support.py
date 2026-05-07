import logging
import requests
from datetime import datetime, timedelta
from requests import Response, Session
from typing import  Optional, Any

from api.http_adapter import HttpAdapter
from adapter_impl import Registry





logger = logging.getLogger(__name__)

class AutoRecreateHttpClient(HttpAdapter):
    def __init__(self, ttl_minutes: int = 30) -> None:
        self.ttl: timedelta = timedelta(minutes=ttl_minutes)
        self.last_created: datetime | None = None
        self.session: Session = self._create_session("http client initialization")

    def _create_session(self, reason: str) -> Session:
        """Closes the old session (if any) and opens a new one."""
        if hasattr(self, 'session') and self.session:
            try:
                self.session.close()
            except Exception:
                pass

        session = requests.Session()
        self.last_created = datetime.now()
        logger.info("New http session created (" + reason +")")
        return session

    def _is_expired(self) -> bool:
        assert self.last_created is not None
        return datetime.now() - self.last_created > self.ttl

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        # 1. Pre-check: Has the time expired?
        if self._is_expired():
            logger.info("TTL reached. Renewing session before request.")
            self.session = self._create_session("ttl reached")

        try:
            # Execute request
            response = self.session.request(method, url, **kwargs)

            # Optional: On 401, also recreate the session for the next call
            if response.status_code == 401:
                logger.warning("Status 401: Session will be renewed for the next call.")
                self.session = self._create_session("session invalid due to former 401 status")

            return response

        except Exception as e:
            # 2. Error-check: Recreate immediately on exception
            logger.exception("Exception caught: %s", e)
            logger.info("Session will be re-initialized for the next attempt.")
            self.session = self._create_session("unknown error of former execution")

            # Pass error directly (no retry)
            raise e

    # Shortcuts
    def get(self, url: str, **kwargs: Any) -> Response:
        return self.request('GET', url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> Response:
        return self.request('POST', url, **kwargs)



class HttpRegistry(Registry):
    NAME = 'http_adapter'

    def __init__(self):
        self.http_client = AutoRecreateHttpClient()

    def get_adapter(self, name: Optional[str] = None) -> Optional[Any]:
        return self.http_client
