"""Redis implementation of the messaging service."""

import asyncio
import logging
from typing import Any, override

from redis.asyncio.client import PubSub, Redis

from viva_api.common.messaging.messaging_service import MessageHandler, MessagingService

logger = logging.getLogger(__name__)


class MessagingServiceRedis(MessagingService):
    """Redis implementation of the messaging service using pub/sub."""

    def __init__(self) -> None:
        """Initialize the Redis messaging service."""
        self._client: Redis | None = None
        self._pubsub: PubSub | None = None
        self._subscriptions: dict[str, MessageHandler] = {}
        self._listener_task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event = asyncio.Event()

    @override
    async def connect(self, host: str, port: int, **kwargs: Any) -> None:
        """Connect to the Redis server.

        Args:
            host: Redis server host (e.g. localhost)
            port: Redis server port (e.g. 6379)
            **kwargs: Additional Redis connection parameters
        """
        if self._client is not None:
            logger.warning("Redis client is already connected")
            return

        logger.info(f"Connecting to Redis server at host:port {host}:{port}")
        self._client = Redis(host=host, port=port, **kwargs)

        # Test the connection
        await self._client.ping()

        self._pubsub = self._client.pubsub()
        logger.info("Successfully connected to Redis server")

    @override
    async def disconnect(self) -> None:
        """Disconnect from the Redis server."""
        if self._listener_task is not None and not self._listener_task.done():
            logger.info("Stopping Redis listener task")
            self._stop_event.set()
            await self._listener_task

        if self._pubsub is not None:
            logger.info("Unsubscribing from all Redis channels")
            await self._pubsub.unsubscribe()
            await self._pubsub.close()
            self._pubsub = None

        if self._client is not None:
            logger.info("Disconnecting from Redis server")
            await self._client.close()
            self._client = None
            logger.info("Disconnected from Redis server")

    @override
    async def publish(self, subject: str, data: bytes) -> None:
        """Publish a message to a Redis channel.

        Args:
            subject: The Redis channel to publish to
            data: The message data as bytes

        Raises:
            RuntimeError: If not connected to Redis
        """
        if self._client is None:
            raise RuntimeError("Not connected to Redis server")

        await self._client.publish(subject, data)
        logger.debug(f"Published message to channel '{subject}'")

    @override
    async def subscribe(self, subject: str, callback: MessageHandler) -> None:
        """Subscribe to a Redis channel with a callback handler.

        Args:
            subject: The Redis channel to subscribe to
            callback: Async function to handle received messages

        Raises:
            RuntimeError: If not connected to Redis
        """
        if self._client is None or self._pubsub is None:
            raise RuntimeError("Not connected to Redis server")

        # Store the callback for this channel
        self._subscriptions[subject] = callback

        # Subscribe to the channel
        await self._pubsub.subscribe(subject)
        logger.info(f"Subscribed to Redis channel '{subject}'")

        # Start the listener task if not already running
        if self._listener_task is None or self._listener_task.done():
            self._stop_event.clear()
            self._listener_task = asyncio.create_task(self._listen_for_messages())

    async def _listen_for_messages(self) -> None:
        """Listen for messages on subscribed channels and dispatch to callbacks."""
        if self._pubsub is None:
            logger.error("PubSub is not initialized")
            return

        logger.info("Starting Redis message listener")
        try:
            while not self._stop_event.is_set():
                try:
                    # Use get_message with timeout to allow periodic checking of stop_event
                    message = await asyncio.wait_for(
                        self._pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0), timeout=1.0
                    )

                    if message is not None and message["type"] == "message":
                        channel = (
                            message["channel"].decode("utf-8")
                            if isinstance(message["channel"], bytes)
                            else message["channel"]
                        )
                        data = message["data"]

                        # Dispatch to the appropriate callback
                        if channel in self._subscriptions:
                            callback = self._subscriptions[channel]
                            try:
                                await callback(data)
                            except Exception:
                                logger.exception(f"Error in message handler for channel '{channel}'")

                except TimeoutError:
                    # Timeout is expected, just continue to check stop_event
                    continue
                except Exception:
                    if not self._stop_event.is_set():
                        logger.exception("Error while listening for Redis messages")
                    break

        except Exception:
            logger.exception("Fatal error in Redis message listener")
        finally:
            logger.info("Redis message listener stopped")

    def is_connected(self) -> bool:
        """Check if connected to Redis server.

        Returns:
            True if connected, False otherwise
        """
        return self._client is not None
