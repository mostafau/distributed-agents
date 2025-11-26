"""
Message bus for distributed communication.

Provides pub/sub messaging for:
- Task distribution
- Result collection
- Worker coordination
- Event broadcasting
"""

import json
import asyncio
from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional, Any, Set
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

logger = logging.getLogger(__name__)


class MessageType(Enum):
    """Types of messages in the system."""
    TASK_SUBMITTED = "task_submitted"
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    WORKER_REGISTERED = "worker_registered"
    WORKER_UNREGISTERED = "worker_unregistered"
    WORKER_HEARTBEAT = "worker_heartbeat"
    WORKFLOW_STARTED = "workflow_started"
    WORKFLOW_COMPLETED = "workflow_completed"
    WORKFLOW_FAILED = "workflow_failed"
    CUSTOM = "custom"


@dataclass
class Message:
    """A message in the system."""
    message_type: MessageType
    payload: Dict[str, Any]
    source: Optional[str] = None
    timestamp: datetime = None
    message_id: Optional[str] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()
        if self.message_id is None:
            import uuid
            self.message_id = str(uuid.uuid4())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_type": self.message_type.value,
            "payload": self.payload,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "message_id": self.message_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        return cls(
            message_type=MessageType(data["message_type"]),
            payload=data["payload"],
            source=data.get("source"),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            message_id=data.get("message_id"),
        )
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> "Message":
        return cls.from_dict(json.loads(json_str))


# Type for message handlers
MessageHandler = Callable[[Message], Any]


class MessageBus(ABC):
    """
    Abstract message bus for distributed communication.
    
    Provides pub/sub messaging between distributed components.
    """
    
    @abstractmethod
    async def connect(self) -> None:
        """Connect to the message bus."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the message bus."""
        pass
    
    @abstractmethod
    async def publish(self, channel: str, message: Message) -> None:
        """Publish a message to a channel."""
        pass
    
    @abstractmethod
    async def subscribe(
        self,
        channels: List[str],
        handler: MessageHandler,
    ) -> None:
        """Subscribe to channels with a handler."""
        pass
    
    @abstractmethod
    async def unsubscribe(self, channels: List[str]) -> None:
        """Unsubscribe from channels."""
        pass


class RedisMessageBus(MessageBus):
    """
    Redis-backed message bus using pub/sub.
    
    Example:
        bus = RedisMessageBus("redis://localhost:6379/0")
        await bus.connect()
        
        # Subscribe to events
        async def handle_task(message):
            print(f"Task event: {message.payload}")
        
        await bus.subscribe(["tasks"], handle_task)
        
        # Publish events
        await bus.publish("tasks", Message(
            message_type=MessageType.TASK_SUBMITTED,
            payload={"task_id": "123"}
        ))
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        prefix: str = "dagent",
    ):
        self.redis_url = redis_url
        self.prefix = prefix
        self._client: Optional[redis.Redis] = None
        self._pubsub: Optional[redis.client.PubSub] = None
        self._handlers: Dict[str, List[MessageHandler]] = {}
        self._listener_task: Optional[asyncio.Task] = None
        self._running = False
    
    def _channel_key(self, channel: str) -> str:
        return f"{self.prefix}:channel:{channel}"
    
    async def connect(self) -> None:
        """Connect to Redis."""
        if redis is None:
            raise ImportError("redis package required: pip install redis")
        
        self._client = redis.from_url(self.redis_url)
        await self._client.ping()
        
        self._pubsub = self._client.pubsub()
        self._running = True
        
        logger.info(f"Message bus connected to Redis at {self.redis_url}")
    
    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        self._running = False
        
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        
        if self._pubsub:
            await self._pubsub.close()
            self._pubsub = None
        
        if self._client:
            await self._client.close()
            self._client = None
        
        logger.info("Message bus disconnected")
    
    async def publish(self, channel: str, message: Message) -> None:
        """Publish a message to a channel."""
        key = self._channel_key(channel)
        await self._client.publish(key, message.to_json())
        logger.debug(f"Published {message.message_type.value} to {channel}")
    
    async def subscribe(
        self,
        channels: List[str],
        handler: MessageHandler,
    ) -> None:
        """Subscribe to channels with a handler."""
        keys = [self._channel_key(ch) for ch in channels]
        
        # Store handlers
        for channel in channels:
            if channel not in self._handlers:
                self._handlers[channel] = []
            self._handlers[channel].append(handler)
        
        # Subscribe to Redis channels
        await self._pubsub.subscribe(*keys)
        
        # Start listener if not already running
        if not self._listener_task or self._listener_task.done():
            self._listener_task = asyncio.create_task(self._listen())
        
        logger.info(f"Subscribed to channels: {channels}")
    
    async def unsubscribe(self, channels: List[str]) -> None:
        """Unsubscribe from channels."""
        keys = [self._channel_key(ch) for ch in channels]
        await self._pubsub.unsubscribe(*keys)
        
        # Remove handlers
        for channel in channels:
            self._handlers.pop(channel, None)
        
        logger.info(f"Unsubscribed from channels: {channels}")
    
    async def _listen(self) -> None:
        """Listen for messages and dispatch to handlers."""
        logger.info("Message listener started")
        
        try:
            while self._running:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0
                )
                
                if message and message["type"] == "message":
                    # Parse channel name
                    full_channel = message["channel"].decode()
                    channel = full_channel.replace(f"{self.prefix}:channel:", "")
                    
                    # Parse message
                    try:
                        msg = Message.from_json(message["data"].decode())
                    except Exception as e:
                        logger.error(f"Failed to parse message: {e}")
                        continue
                    
                    # Dispatch to handlers
                    handlers = self._handlers.get(channel, [])
                    for handler in handlers:
                        try:
                            if asyncio.iscoroutinefunction(handler):
                                await handler(msg)
                            else:
                                handler(msg)
                        except Exception as e:
                            logger.error(f"Handler error: {e}")
        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Listener error: {e}")
        
        logger.info("Message listener stopped")
    
    # Convenience methods for common message types
    
    async def publish_task_submitted(
        self,
        task_id: str,
        workflow_id: str,
        node_name: str,
        **extra
    ) -> None:
        """Publish task submitted event."""
        await self.publish("tasks", Message(
            message_type=MessageType.TASK_SUBMITTED,
            payload={
                "task_id": task_id,
                "workflow_id": workflow_id,
                "node_name": node_name,
                **extra
            }
        ))
    
    async def publish_task_completed(
        self,
        task_id: str,
        workflow_id: str,
        node_name: str,
        worker_id: str,
        **extra
    ) -> None:
        """Publish task completed event."""
        await self.publish("tasks", Message(
            message_type=MessageType.TASK_COMPLETED,
            payload={
                "task_id": task_id,
                "workflow_id": workflow_id,
                "node_name": node_name,
                "worker_id": worker_id,
                **extra
            }
        ))
    
    async def publish_task_failed(
        self,
        task_id: str,
        workflow_id: str,
        node_name: str,
        error: str,
        **extra
    ) -> None:
        """Publish task failed event."""
        await self.publish("tasks", Message(
            message_type=MessageType.TASK_FAILED,
            payload={
                "task_id": task_id,
                "workflow_id": workflow_id,
                "node_name": node_name,
                "error": error,
                **extra
            }
        ))
    
    async def publish_worker_heartbeat(
        self,
        worker_id: str,
        current_load: int,
        status: str,
    ) -> None:
        """Publish worker heartbeat."""
        await self.publish("workers", Message(
            message_type=MessageType.WORKER_HEARTBEAT,
            payload={
                "worker_id": worker_id,
                "current_load": current_load,
                "status": status,
            },
            source=worker_id,
        ))
    
    async def publish_workflow_completed(
        self,
        workflow_id: str,
        graph_name: str,
        **extra
    ) -> None:
        """Publish workflow completed event."""
        await self.publish("workflows", Message(
            message_type=MessageType.WORKFLOW_COMPLETED,
            payload={
                "workflow_id": workflow_id,
                "graph_name": graph_name,
                **extra
            }
        ))


class LocalMessageBus(MessageBus):
    """
    In-memory message bus for testing and single-process deployments.
    """
    
    def __init__(self):
        self._handlers: Dict[str, List[MessageHandler]] = {}
        self._running = False
    
    async def connect(self) -> None:
        self._running = True
    
    async def disconnect(self) -> None:
        self._running = False
        self._handlers.clear()
    
    async def publish(self, channel: str, message: Message) -> None:
        handlers = self._handlers.get(channel, [])
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(message)
                else:
                    handler(message)
            except Exception as e:
                logger.error(f"Handler error: {e}")
    
    async def subscribe(
        self,
        channels: List[str],
        handler: MessageHandler,
    ) -> None:
        for channel in channels:
            if channel not in self._handlers:
                self._handlers[channel] = []
            self._handlers[channel].append(handler)
    
    async def unsubscribe(self, channels: List[str]) -> None:
        for channel in channels:
            self._handlers.pop(channel, None)
