"""Monotonic virtual time; speed affects pacing only, never payload timestamps."""
import time
from datetime import datetime


class ReplayClock:
    def __init__(self, start: datetime, speed="max", sleeper=time.sleep, monotonic=time.monotonic):
        self.current = start
        self.origin = start
        self.speed = None if str(speed) == "max" else float(speed)
        if self.speed is not None and (not 0 < self.speed < float("inf")):
            raise ValueError("speed must be max or finite positive multiplier")
        self.sleep, self.wall = sleeper, monotonic
        self.wall_start = monotonic()

    def advance(self, stamp):
        self.current = max(self.current, stamp)
        if self.speed is not None:
            deadline = (self.current - self.origin).total_seconds() / self.speed
            wait = deadline - (self.wall() - self.wall_start)
            if wait > 0:
                self.sleep(wait)
        return self.current

    def now(self):
        return self.current

    def monotonic(self):
        return (self.current - self.origin).total_seconds()
