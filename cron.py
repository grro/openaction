import logging
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Thread
from time import sleep
from typing import Dict

from api.http_service import HttpClient
from mcp_client import McpRegistry
from store import Store
from task import CronTaskAdapter



class CronService:

    def __init__(self, store: Store, mcp_registry: McpRegistry, task_registry : TaskRegistry, http_session: HttpClient):
        self.is_running = False
        self.store = store
        self.mcp_registry = mcp_registry
        self.http_session = http_session
        self.task_registry = task_registry
        self._cron_cache: Dict[str, set[int]] = {}
        self._executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="CronWorker")

    def __str__(self):
        return f"CronService(jobs={len(self.task_registry.tasks)})\n\r" + "\n\r".join([" * " + str(task) for task in self.task_registry.tasks])

    def stop(self):
        self.is_running = False
        return self

    def start(self):
        self.is_running = True
        Thread(target=self.__loop, daemon=True).start()

    def __loop(self):
        while self.is_running:
            now = datetime.now()
            run_key = (now.year, now.month, now.day, now.hour, now.minute)

            for task in list(self.task_registry.tasks.values()):
                try:
                    if self._should_run(task, now, run_key):
                        self._executor.submit(self._safe_run, task)
                except Exception:
                    logging.exception(f"Error triggering task: {task.name}")

            next_tick = (datetime.now() + timedelta(seconds=1)).replace(microsecond=0)
            sleep_time = (next_tick - datetime.now()).total_seconds()
            sleep(sleep_time)

    def _safe_run(self, task: CronTaskAdapter):
        """Executes the task and ensures any unhandled exceptions are logged."""
        try:
            task.run(self.store, self.mcp_registry, self.http_session)
        except Exception as e:
            logging.error(f"Execution failed for task '{task.name}': {e}")

    def _should_run(self, task: CronTaskAdapter, now: datetime, run_key: tuple[int, int, int, int, int]) -> bool:
        # If task failed recently, wait 1 minute before retrying
        time_since_error = task.time_since_last_error()
        if time_since_error is not None:
            if time_since_error < timedelta(minutes=1):
                return False

        # Check if already run in this minute or if cron expression matches
        last_run = task.time_last_run()
        if last_run is not None:
            if (last_run.year, last_run.month, last_run.day, last_run.hour, last_run.minute) == run_key:
                return False
        return self._matches(task.cron_expression, now)

    def _validate_cron_expression(self, expression: str) -> None:
        fields = expression.split()
        if len(fields) != 5:
            raise ValueError(f"Invalid cron expression '{expression}'. Expected 5 fields.")

        ranges = [
            (0, 59),
            (0, 23),
            (1, 31),
            (1, 12),
            (0, 7),
        ]
        for field, (minimum, maximum) in zip(fields, ranges, strict=True):
            self._parse_field(field, minimum, maximum)

    def _matches(self, expression: str, now: datetime) -> bool:
        """Splits the cron expression and evaluates each field."""
        try:
            minute, hour, day, month, weekday = expression.split()
            cron_weekday = (now.weekday() + 1) % 7 # ISO (Mon=0) to Cron (Sun=0/7)

            # Pass unique cache keys for each field type
            return (
                    self._matches_field(minute, now.minute, 0, 59, "m")
                    and self._matches_field(hour, now.hour, 0, 23, "h")
                    and self._matches_field(day, now.day, 1, 31, "d")
                    and self._matches_field(month, now.month, 1, 12, "M")
                    and self._matches_field(weekday, cron_weekday, 0, 7, "w")
            )
        except ValueError:
            logging.error(f"Invalid cron expression encountered: {expression}")
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