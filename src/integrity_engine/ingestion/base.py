"""Common lifecycle contract for asynchronous exchange ingestors."""

from abc import ABC, abstractmethod


class ExchangeIngestor(ABC):
    """Run until cancelled; implementations must release network resources."""

    @abstractmethod
    async def run(self) -> None:
        """Receive and publish raw exchange events until cancellation or failure."""
