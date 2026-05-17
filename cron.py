import logging
from datetime import datetime, timedelta
from typing import Dict, Optional


logger = logging.getLogger(__name__)


class CronExpression:

    def __init__(self, expression: str):
        self.expression = expression
        self._cron_cache: Dict[str, set[int]] = {}

    def should_run(self, last_attempt_at: Optional[datetime], last_failure_age: Optional[timedelta]) -> bool:
        if self.expression is None:
            return False
        # If task failed recently, wait 1 minute before retrying
        time_since_error = last_failure_age
        if time_since_error is not None:
            if time_since_error < timedelta(minutes=1):
                return False

        # Build dedup key matching the cron's resolution (per-second for 6-field, per-minute for 5-field).
        has_seconds = len(self.expression.split()) == 6
        now = datetime.now()
        if has_seconds:
            run_key = (now.year, now.month, now.day, now.hour, now.minute, now.second)
        else:
            run_key = (now.year, now.month, now.day, now.hour, now.minute)

        # Skip if we already triggered the task within the same dedup window
        last_run = last_attempt_at
        if last_run is not None:
            if has_seconds:
                last_key = (last_run.year, last_run.month, last_run.day, last_run.hour, last_run.minute, last_run.second)
            else:
                last_key = (last_run.year, last_run.month, last_run.day, last_run.hour, last_run.minute)
            if last_key == run_key:
                return False
        return self._matches(self.expression, now)

    def validate(self, expression: str) -> None:
        fields = expression.split()
        if len(fields) not in (5, 6):
            raise ValueError(f"Invalid cron expression '{expression}'. Expected 5 or 6 fields.")

        ranges_5 = [
            (0, 59),  # minute
            (0, 23),  # hour
            (1, 31),  # day
            (1, 12),  # month
            (0, 7),   # weekday
        ]
        ranges_6 = [(0, 59)] + ranges_5  # prepend seconds

        ranges = ranges_6 if len(fields) == 6 else ranges_5
        for field, (minimum, maximum) in zip(fields, ranges, strict=True):
            self._parse_field(field, minimum, maximum)

    def _matches(self, expression: Optional[str], now: datetime) -> bool:
        """Splits the cron expression and evaluates each field. Supports 5-field (m h d M w) and 6-field (s m h d M w)."""
        if not expression:
            return False
        try:
            fields = expression.split()
            if len(fields) == 6:
                second, minute, hour, day, month, weekday = fields
            elif len(fields) == 5:
                second = "0"  # match only at second 0 for minute-resolution crons
                minute, hour, day, month, weekday = fields
            else:
                raise ValueError(f"Expected 5 or 6 fields, got {len(fields)}")

            cron_weekday = (now.weekday() + 1) % 7  # ISO (Mon=0) to Cron (Sun=0/7)

            return (
                    self._matches_field(second, now.second, 0, 59, "s")
                    and self._matches_field(minute, now.minute, 0, 59, "m")
                    and self._matches_field(hour, now.hour, 0, 23, "h")
                    and self._matches_field(day, now.day, 1, 31, "d")
                    and self._matches_field(month, now.month, 1, 12, "M")
                    and self._matches_field(weekday, cron_weekday, 0, 7, "w")
            )
        except ValueError:
            logger.error(f"Invalid cron expression encountered: {expression}")
            return False

    def _matches_field(self, field: str, value: int, minimum: int, maximum: int, cache_key_part: str) -> bool:
        """
        Checks a specific time field (e.g., 'minute') against its cron part.
        Uses caching to avoid re-parsing identical expressions across tasks.
        """
        cache_key = f"{cache_key_part}:{field}:{minimum}:{maximum}"
        if cache_key not in self._cron_cache:
            self._cron_cache[cache_key] = self._parse_field(field, minimum, maximum)

        allowed_values = self._cron_cache[cache_key]

        # Special case for Sunday: support both 0 and 7.
        if maximum == 7 and value == 0 and 7 in allowed_values:
            return True
        return value in allowed_values

    def _parse_field(self, field: str, minimum: int, maximum: int) -> set[int]:
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
                start = minimum
                end = maximum
            elif "-" in base:
                start_text, end_text = base.split("-", 1)
                start = int(start_text)
                end = int(end_text)
            else:
                start = int(base)
                end = int(base)

            if start < minimum or end > maximum or start > end:
                raise ValueError(f"Cron value '{part}' out of range {minimum}-{maximum}")

            values.update(range(start, end + 1, step))

        return values