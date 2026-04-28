import requests
from datetime import datetime, timedelta
from typing import Any
from requests import Response, Session

from api.http_service import HttpClient



class AutoRecreateHttpClient(HttpClient):
    def __init__(self, ttl_minutes: int = 30) -> None:
        self.ttl: timedelta = timedelta(minutes=ttl_minutes)
        self.last_created: datetime | None = None
        self.session: Session = self._create_session()

    def _create_session(self) -> Session:
        """Schließt die alte Session (falls vorhanden) und öffnet eine neue."""
        if hasattr(self, 'session') and self.session:
            try:
                self.session.close()
            except Exception:
                pass

        session = requests.Session()
        self.last_created = datetime.now()
        print(f"[{self.last_created.strftime('%H:%M:%S')}] --- Neue Session erstellt ---")
        return session

    def _is_expired(self) -> bool:
        assert self.last_created is not None
        return datetime.now() - self.last_created > self.ttl

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        # 1. Vorab-Check: Zeit abgelaufen?
        if self._is_expired():
            print("TTL erreicht. Erneuere Session vor Request.")
            self.session = self._create_session()

        try:
            # Request ausführen
            response = self.session.request(method, url, **kwargs)

            # Optional: Bei 401 ebenfalls für das nächste Mal neu aufsetzen
            if response.status_code == 401:
                print("Status 401: Session wird für den nächsten Aufruf erneuert.")
                self.session = self._create_session()

            return response

        except Exception as e:
            # 2. Fehler-Check: Bei Exception sofort neu erzeugen
            print(f"Exception abgefangen: {e}")
            print("Session wird für den nächsten Versuch neu initialisiert.")
            self.session = self._create_session()

            # Fehler direkt weitergeben (kein Retry)
            raise e

    # Shortcuts
    def get(self, url: str, **kwargs: Any) -> Response:
        return self.request('GET', url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> Response:
        return self.request('POST', url, **kwargs)
