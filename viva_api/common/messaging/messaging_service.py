"""Abstract messaging service interface for pub/sub messaging."""

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any

# Type alias for message handler callbacks
MessageHandler = Callable[[bytes], Awaitable[None]]


class MessagingService(ABC):
    """Abstract base class for messaging services (Redis, etc.)."""

    @abstractmethod
    async def connect(self, host: str, port: int, **kwargs: Any) -> None:
        """Connect to the messaging service.

        Args:
            host: Connection host for the messaging service
            port: Connection port for the messaging service
            **kwargs: Additional connection parameters
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the messaging service."""
        pass

    @abstractmethod
    async def publish(self, subject: str, data: bytes) -> None:
        """Publish a message to a subject/channel.

        Args:
            subject: The subject/channel to publish to
            data: The message data as bytes
        """
        pass

    @abstractmethod
    async def subscribe(self, subject: str, callback: MessageHandler) -> None:
        """Subscribe to a subject/channel with a callback handler.

        Args:
            subject: The subject/channel to subscribe to
            callback: Async function to handle received messages
        """
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the messaging service is currently connected.

        Returns:
            True if connected, False otherwise
        """
        pass
