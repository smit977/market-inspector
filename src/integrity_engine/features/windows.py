"""Reusable bounded event-time windows."""
from collections import deque
from datetime import datetime, timedelta
from typing import Generic, TypeVar
T = TypeVar("T")
class EventWindow(Generic[T]):
    def __init__(self, max_age: timedelta): self.max_age, self.items = max_age, deque()
    def add(self, timestamp: datetime, value: T) -> None:
        self.items.append((timestamp, value)); self.expire(timestamp)
    def expire(self, now: datetime) -> None:
        cutoff = now - self.max_age
        while self.items and self.items[0][0] < cutoff: self.items.popleft()
    def values(self, start: datetime, end: datetime):
        return [v for t, v in self.items if start <= t <= end]
    def __len__(self): return len(self.items)
