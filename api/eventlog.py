from abc import ABC, abstractmethod


class EventLog(ABC):
    """
    A specialized logger designed to retain the most significant daily events for a short duration.

    This log is intended strictly for high-priority occurrences rather than verbose debugging
    (e.g., a roller shutter changing position, or a heater being switched on/off).
    To maintain a high signal-to-noise ratio, the volume of logged events should typically
    not exceed 30 per day. Log entries are automatically purged after a specified retention
    period (default: 14 days).
    """

    @abstractmethod
    def log_event(self, topic: str, text: str, ttl: int = 14 * 24 * 60 * 60) -> None:
        """
        Records an important event to the daily log.

        Args:
            topic (str): The category or subject of the event (e.g., "Security", "System").
            text (str): A descriptive message detailing the occurrence.
            ttl (int, optional): Time-to-live in seconds. Defaults to 1209600 (14 days).
        """
        pass