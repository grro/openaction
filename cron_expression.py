import logging
from datetime import datetime, timedelta
from typing import Dict, Optional


logger = logging.getLogger(__name__)



class CronExpression:
    """
    Parses and evaluates a standard 5-field cron expression: `minute hour day month weekday`.

    Field ranges:
        minute  : 0-59
        hour    : 0-23
        day     : 1-31
        month   : 1-12
        weekday : 0-7 (both 0 and 7 represent Sunday)

    Supports `*`, ranges (`a-b`), lists (`a,b,c`) and steps (`*/n`, `a-b/n`).
    """

    # (minimum, maximum) for each of the 5 fields, in order.
    _FIELD_RANGES = [
        (0, 59),  # minute
        (0, 23),  # hour
        (1, 31),  # day
        (1, 12),  # month
        (0, 7),   # weekday (0 and 7 both mean Sunday)
    ]
    _FIELD_KEYS = ("m", "h", "d", "M", "w")

    def __init__(self, expression: str):
        self.expression = (expression or "").strip()
        self._cron_cache: Dict[str, set[int]] = {}

    def should_run(self,
                   last_attempt_at: Optional[datetime],
                   last_failure_at: Optional[datetime]) -> bool:
        """
        Returns True when the cron expression matches the current minute and the task
        has not yet been triggered in this minute. After a failure, retries are throttled
        to at most once per minute.
        """
        if not self.expression:
            return False

        now = datetime.now()

        # Throttle retries after a recent failure.
        if last_failure_at is not None and (now - last_failure_at) < timedelta(minutes=1):
            return False

        run_key = (now.year, now.month, now.day, now.hour, now.minute)

        # Skip if we already triggered the task within the same minute.
        if last_attempt_at is not None:
            last_key = (last_attempt_at.year, last_attempt_at.month, last_attempt_at.day,
                        last_attempt_at.hour, last_attempt_at.minute)
            if last_key == run_key:
                return False

        return self._matches(now)

    def validate(self, expression: str) -> None:
        """Raises ValueError if `expression` is not a valid 5-field cron expression."""
        fields = expression.split()
        if len(fields) != 5:
            raise ValueError(f"Invalid cron expression '{expression}'. Expected 5 fields.")
        for field, (minimum, maximum) in zip(fields, self._FIELD_RANGES, strict=True):
            self._parse_field(field, minimum, maximum)

    def _matches(self, now: datetime) -> bool:
        """Evaluates each cron field against the components of `now`."""
        try:
            fields = self.expression.split()
            if len(fields) != 5:
                raise ValueError(f"Expected 5 fields, got {len(fields)}")

            cron_weekday = (now.weekday() + 1) % 7  # ISO (Mon=0) -> Cron (Sun=0/7)
            values = (now.minute, now.hour, now.day, now.month, cron_weekday)

            for field, value, (minimum, maximum), key in zip(
                    fields, values, self._FIELD_RANGES, self._FIELD_KEYS, strict=True):
                if not self._matches_field(field, value, minimum, maximum, key):
                    return False
            return True
        except ValueError:
            logger.error(f"Invalid cron expression encountered: {self.expression}")
            return False

    def _matches_field(self, field: str, value: int, minimum: int, maximum: int, cache_key_part: str) -> bool:
        """Checks one cron field against the corresponding time component, using a parse cache."""
        cache_key = f"{cache_key_part}:{field}:{minimum}:{maximum}"
        if cache_key not in self._cron_cache:
            self._cron_cache[cache_key] = self._parse_field(field, minimum, maximum)

        allowed_values = self._cron_cache[cache_key]

        # Special case for Sunday: both 0 and 7 are accepted.
        if maximum == 7 and value == 0 and 7 in allowed_values:
            return True
        return value in allowed_values

    @staticmethod
    def _parse_field(field: str, minimum: int, maximum: int) -> set[int]:
        """Expands a single cron field (e.g. `*/5`, `1-5`, `1,3,5`) into the matching set of ints."""
        values: set[int] = set()

        for part in field.split(","):
            part = part.strip()
            if not part:
                raise ValueError("Empty cron field part")

            if "/" in part:
                base, step_text = part.split("/", 1)
                step = int(step_text)
                if step <= 0:
                    raise ValueError(f"Invalid cron step '{part}'")
            else:
                base = part
                step = 1

            if base == "*":
                start, end = minimum, maximum
            elif "-" in base:
                start_text, end_text = base.split("-", 1)
                start, end = int(start_text), int(end_text)
            else:
                start = end = int(base)

            if start < minimum or end > maximum or start > end:
                raise ValueError(f"Cron value '{part}' out of range {minimum}-{maximum}")

            values.update(range(start, end + 1, step))

        return values
