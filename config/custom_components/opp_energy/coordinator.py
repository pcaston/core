import asyncio  # noqa: D100
from datetime import timedelta
import json
import logging

from websockets.client import WebSocketClientProtocol, connect
from websockets.exceptions import WebSocketException

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import DEFAULT_CLOUD_URL, DOMAIN, UPDATE_INTERVAL

_LOGGER = logging.getLogger(__name__)

class OppEnergyDataUpdateCoordinator(DataUpdateCoordinator):
    """Class to manage fetching OPP Energy data."""

    def __init__(
        self,
        hass: HomeAssistant,
        user_name: str,
        email: str,
        password: str,
        site_name: str,
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=UPDATE_INTERVAL),
        )
        self.websocket_url = DEFAULT_CLOUD_URL
        self.user_name = user_name
        self.email = email
        self.password = password
        self.site_name = site_name
        self.ws: WebSocketClientProtocol | None = None
        self._authenticated = False
        self._last_data = None
        self._reconnect_task = None
        self._keep_alive_task = None
        self._listener_task = None
        self.instance_id = f"opp_energy_{email}_{site_name}"

        # Message queue for synchronization
        self._message_queue = asyncio.Queue()
        self._response_futures = {}
        self._next_message_id = 0

    def _is_connected(self) -> bool:
        """Check if the WebSocket connection is active."""
        return self.ws is not None and not self.ws.closed

    async def _connect(self) -> None:
        """Establish WebSocket connection."""
        if self._is_connected():
            return

        try:
            _LOGGER.debug("Attempting to connect to WebSocket at: %s", self.websocket_url)
            self.ws = await connect(
                self.websocket_url,
                ping_interval=60,
                ping_timeout=20,
            )
            _LOGGER.debug("WebSocket connection established")

            # Start message listener task
            if self._listener_task is None or self._listener_task.done():
                self._listener_task = asyncio.create_task(self._listen_for_messages())

            # Start keep-alive task
            if self._keep_alive_task is None or self._keep_alive_task.done():
                self._keep_alive_task = asyncio.create_task(self._keep_alive())

            if not self._authenticated:
                await self._authenticate()

        except WebSocketException as error:
            self.ws = None
            raise UpdateFailed(f"Failed to connect to WebSocket: {error}") from error
        except Exception as error:
            self.ws = None
            raise UpdateFailed(f"Unexpected error during connection: {error}") from error

    async def _listen_for_messages(self) -> None:
        """Listen for incoming WebSocket messages continuously."""
        while self._is_connected():
            try:
                message = await self.ws.recv()
                parsed_message = json.loads(message)
                _LOGGER.debug("Received message: %s", parsed_message)

                # Handle the message based on its type
                msg_type = parsed_message.get("type")

                if msg_type == "pong":
                    # Handle pong directly - no need to process further
                    _LOGGER.debug("Received pong response")
                    continue

                if msg_type == "auth_success":
                    # Handle authentication success
                    _LOGGER.debug("Authentication successful")
                    self._authenticated = True
                    continue

                if msg_type == "price_update":
                    # Process price updates
                    _LOGGER.debug("Received price data")
                    price_data = parsed_message.get("data", {})
                    if not price_data and "buy_price" in parsed_message:
                        # Handle case where price data is in root of message
                        price_data = {
                            "buy_price": parsed_message.get("buy_price"),
                            "sell_price": parsed_message.get("sell_price"),
                            "timestamp": parsed_message.get("timestamp")
                        }

                    # Update our last data and notify entities
                    if price_data:
                        self._last_data = price_data
                        self.async_set_updated_data(self._last_data)
                    continue

                # For other message types, check if it's a response to a pending request
                message_id = parsed_message.get("id")
                if message_id and message_id in self._response_futures:
                    # This is a response to a specific request
                    future = self._response_futures.pop(message_id)
                    future.set_result(parsed_message)

            except Exception as error:  # noqa: BLE001
                _LOGGER.error("Error in message listener: %s", error)
                self.ws = None
                # Schedule reconnection
                if self._reconnect_task is None or self._reconnect_task.done():
                    self._reconnect_task = asyncio.create_task(self._reconnect())
                break

    async def _keep_alive(self) -> None:
        """Keep the WebSocket connection alive."""
        while True:
            try:
                if self._is_connected():
                    await self._send_message_without_response({"type": "ping"})
                # Try to reconnect if connection is lost
                elif self._reconnect_task is None or self._reconnect_task.done():
                    self._reconnect_task = asyncio.create_task(self._reconnect())
                await asyncio.sleep(30)  # Send message every 30 seconds
            except Exception as e:  # noqa: BLE001
                _LOGGER.error("Keep-alive error: %s", e)
                self.ws = None
                await asyncio.sleep(60)  # Wait before retrying

    async def _reconnect(self) -> None:
        """Handle reconnection to WebSocket server."""
        retry_count = 0
        max_retries = 5
        retry_delay = 5  # seconds

        while retry_count < max_retries:
            _LOGGER.info("Attempting to reconnect to WebSocket (attempt %s of %s)",
                        retry_count + 1, max_retries)
            try:
                await self._connect()
                if self._is_connected():
                    _LOGGER.info("Successfully reconnected to WebSocket")
                    return
            except Exception as error:  # noqa: BLE001
                _LOGGER.error("Reconnection attempt failed: %s", error)

            retry_count += 1
            await asyncio.sleep(retry_delay)

        _LOGGER.error("Failed to reconnect after %s attempts", max_retries)

    async def _send_message_without_response(self, message: dict) -> None:
        """Send a message through the WebSocket connection without expecting a response."""
        if not self._is_connected():
            await self._connect()

        try:
            await self.ws.send(json.dumps(message))
            _LOGGER.debug("Sent message: %s", message)
        except Exception as error:
            _LOGGER.error("Error sending message: %s", error)
            self.ws = None
            raise UpdateFailed(f"Failed to send message: {error}") from error

    async def _send_message_with_response(self, message: dict, timeout: int = 5) -> dict:
        """Send a message and wait for a response with matching ID."""
        if not self._is_connected():
            await self._connect()

        try:
            # Add a unique ID to the message
            message_id = self._next_message_id
            self._next_message_id += 1
            message["id"] = message_id

            # Create a future to receive the response
            response_future = asyncio.Future()
            self._response_futures[message_id] = response_future

            # Send the message
            await self.ws.send(json.dumps(message))
            _LOGGER.debug("Sent message with ID %s: %s", message_id, message)

            # Wait for response with timeout
            try:
                return await asyncio.wait_for(response_future, timeout)
            except TimeoutError:
                self._response_futures.pop(message_id, None)
                raise UpdateFailed(f"Timeout waiting for response to message ID {message_id}")  # noqa: B904

        except Exception as error:
            _LOGGER.error("Error sending message: %s", error)
            self.ws = None
            raise UpdateFailed(f"Failed to send message: {error}") from error

    async def _authenticate(self) -> None:
        """Authenticate with the WebSocket server."""
        if not self._is_connected():
            await self._connect()

        try:
            auth_message = {
                "type": "authenticate",
                "user_name": self.user_name,
                "email": self.email,
                "password": self.password,
                "site_name": self.site_name
            }
            _LOGGER.debug("Sending authentication message")

            # Authentication doesn't use the ID mechanism, so we send without response
            await self._send_message_without_response(auth_message)

            # The listener task will set self._authenticated if auth succeeds
            # Wait for authentication to complete or timeout
            start_time = asyncio.get_event_loop().time()
            while not self._authenticated:
                await asyncio.sleep(0.1)
                if asyncio.get_event_loop().time() - start_time > 5:  # 5 seconds timeout
                    raise UpdateFailed("Authentication timed out")  # noqa: TRY301

            # Subscribe to price updates after successful authentication
            await self._send_message_without_response({
                "type": "subscribe_prices",
                "user_name": self.email
            })
            _LOGGER.debug("Subscribed to price updates")

        except Exception as error:
            self._authenticated = False
            _LOGGER.error("Authentication error: %s", error)
            self.ws = None
            raise UpdateFailed(f"Authentication error: {error}") from error

    async def _async_update_data(self) -> dict:
        """Check connection health and return latest data."""
        try:
            if not self._is_connected():
                await self._connect()

            # If we don't have data yet, request it explicitly
            if self._last_data is None:
                # Only request data if we're authenticated
                if self._authenticated:
                    await self._send_message_without_response({
                        "type": "get_prices",
                        "user_name": self.email
                    })
                    # Wait briefly for a response which will be handled by the listener
                    await asyncio.sleep(1)
                else:
                    _LOGGER.warning("Not requesting prices because not authenticated")

            return self._last_data or {}  # noqa: TRY300

        except Exception as error:
            _LOGGER.error("Error in update coordinator: %s", error)
            if self._last_data is not None:
                return self._last_data
            raise UpdateFailed(f"Failed to update data: {error}") from error

    async def async_shutdown(self) -> None:
        """Close WebSocket connection on shutdown."""
        _LOGGER.debug("Shutting down OPP Energy coordinator")

        # Cancel background tasks
        if self._keep_alive_task and not self._keep_alive_task.done():
            self._keep_alive_task.cancel()

        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()

        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()

        # Clean up response futures
        for future in self._response_futures.values():
            if not future.done():
                future.cancel()
        self._response_futures.clear()

        # Close WebSocket connection
        if self.ws is not None and not self.ws.closed:
            await self.ws.close()
            self.ws = None
